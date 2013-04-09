import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf._
import edu.stanford.nlp.io.IOUtils
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.CoreAnnotations.AnswerAnnotation
import scala.collection.mutable.PriorityQueue
import org.lemurproject.galago.core.parse.{Document,Tag,TagTokenizer}
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.immutable.Set
import scala.collection.Map

import java.net.URL
import scala.io.Source._
import java.io.IOException

object RelModel extends App {
  val graph = QueryGraph()
  graph.addIndex('aquaint)
  graph.add("""\w+""".r.findAllIn(query).toList.map(t => Dirichlet(Term(t))))
  val executor = ExecutionGraph(graph)
  val resultQueue = executor.run


  // These are the LM scorers - will need them later
  val lmScorers = unigrams()

  // Scoring loop for first pass
  var resultQueue = standardScoringLoop(lmScorers, iterators, lengths)
  val fbDocs = 10  // make proper variable later

  // Trickier part - set up for 2nd run

  // take fbDocs
  var initialResults = ListBuffer[ScoredDocument]()
  while (initialResults.size < fbDocs && !resultQueue.isEmpty)
    initialResults += resultQueue.dequeue

  // now recover "probabilities" of each doc
  val max = initialResults.map(_.score).max
  val logSumExp = max + scala.math.log(initialResults.map { v =>
    scala.math.exp(v.score - max)
  }.sum)
  initialResults = initialResults.map { sd =>
    ScoredDocument(sd.docid, scala.math.exp(sd.score - logSumExp))
  }

  // get the actual documents, and count the grams
  import org.lemurproject.galago.core.parse.Document
  import org.lemurproject.galago.core.parse.TagTokenizer
  val tokenizer = new TagTokenizer()
  val dummy = new Parameters()
  // load and tokenize docs
  val docs = initialResults.map { SD =>
    val d = index.getDocument(index.getName(SD.docid), dummy)
    if (d.terms == null || d.terms.size == 0) tokenizer.tokenize(d)
    d // tokenize method returns void/Unit
  }

  // Get stopwords to filter
  val stopwords = Stopwords.inquery

  // histograms of the # of occurrences - each doc is a histogram
  val hists = docs.map( d => (d.identifier, histogram(d)) ).toMap

  // Set of fb terms
  var terms = hists.values map(_.counts.keySet) reduceLeft { (A,B) => A ++ B }
  // that are NOT stopwords
  terms = terms.filterNot(stopwords(_))
  // that are NOT 1-character or less
  terms = terms.filterNot(_.size <= 1)
  // and are NOT all digits
  terms = terms.filterNot(isAllDigits(_))

  // Apparently we need lengths too. Makes (docid -> length) map
  val doclengths = initialResults.map(_.docid).map {
    A => (A, index.getLength(A))
  }.toMap

  // Time to score the terms
  val grams = terms.map { T =>
    // map to score-per-doc then sum
    val score = initialResults.map { SD =>
      val tf =
        hists(SD.docid).counts.getOrElse(T, 0).toDouble / doclengths(SD.docid)
      SD.score * tf
    }.sum
    Gram(T, score)
  }

  // Sort and keep top "fbTerms"
  val fbTerms = 20
  // selectedGrams = term -> Gram map
  val selectedGrams =
    grams.toList.sorted(GramOrdering).take(fbTerms).map(g => (g.term, g)).toMap

  // Need to open new iterators - only open the ones we don't already have
  // rmNodes = term -> Node map
  val rmNodes =
    selectedGrams.values.map { g =>
      (g.term, formatNode(g.term, "counts"))
    } filterNot { pair => nodeMap.contains(pair._2) }

  val rmNodeMap = LinkedHashMap[Node, java.lang.Object]()
  for ((term, node) <- rmNodes) { rmNodeMap.update(node,
    index.getIterator(node).asInstanceOf[PositionIndexReader#TermCountIterator])
  }

  val rmScorers = rmNodes.map { case (term, node) =>
      val scorer = dirichlet(collectionFrequency(node, index, lengths))
      val ps = ParameterizedScorer(scorer, lengths, rmNodeMap(node))
      ps.weight = selectedGrams(term).score
      ps
  }.toList

  val finalScorers = List(
    ParameterizedScorer(lmScorers, 0.7),
    ParameterizedScorer(rmScorers, 0.3)
  )

  val finalIterators = (rmNodeMap.values.toList.map(obj2movableIt(_)) ++
    iterators).toSet.toList
  // Make sure everything's ready to go
  finalIterators.foreach(_.reset)
  lengths.reset

  // Scoring loop
  resultQueue = standardScoringLoop(finalScorers, finalIterators, lengths)

  // Get doc names and print
  printResults(resultQueue, index)
}
