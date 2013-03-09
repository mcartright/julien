import org.lemurproject.galago.core.index.disk.DiskIndex
import org.lemurproject.galago.core.retrieval.iterator.MovableLengthsIterator
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator
import org.lemurproject.galago.core.retrieval.query.{StructuredQuery, Node}
import org.lemurproject.galago.core.index.disk.PositionIndexReader
import org.lemurproject.galago.core.parse.Document
import org.lemurproject.galago.core.parse.TagTokenizer
import org.lemurproject.galago.tupleflow.Utility
import org.lemurproject.galago.tupleflow.Parameters
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.LinkedHashMap
import scala.collection.JavaConversions._
import scala.collection.immutable.Set

// Hack for now to load some common definitions
import GalagoBridging._

// let's load some stopwords
val stopwords = Stopwords.inquery

val index = Sources.aquaint
val dummyParams = new Parameters()
val queryDoc = index.getDocument("NYT19980609.0018", dummyParams)

// formulate a query from the document
// let's...start by getting the headline terms
val headlineTerms =
  """(?s)<HEADLINE>(.*)</HEADLINE>""".r.findFirstMatchIn(queryDoc.text) match {
    case Some(m) => {
      val group = m.group(1)
      var terms = group.replaceAll("""\s""", " ").split(" ").toList
      terms = terms.filterNot(stopwords(_))
      terms = terms.filter(_.size > 1)
      terms.map(_.toLowerCase)
    }
    case None => List.empty
  }

// pull all quotes in the document, (we're going to turn them into
// unordered windows, but remove punctuation and stopwords in the groups)
val quotes =
  """(?s)``(.*?)''""".r.findAllIn(queryDoc.text).matchData.toList.map { m =>
    // Extract the matched group
    m.group(1)
  } map {
    // Scrub as one whole string
    scrub(_)
  } map {
    // And split on WS and make sets (remove dupes)
    _.split("\\s").toSet
  } map {
    // remove any stopwords, short words, and numbers
    _.filterNot(s => stopwords(s) || s.size <= 1 || isAllDigits(s))
  } filter { set : Set[String] =>
  // And keep non-empty sets
    set.size > 0
  } map {
    // And finally turn each set into a list
    _.toList
  }

// Finally, let's pull all pairs of consecutive words that are capitalized
// and pretend they're names :)
val namePattern = """(?s)[A-Z][a-z]+\s(Mc)?[A-Z][a-z]+""".r
val namesMatches = namePattern.findAllIn(queryDoc.text).toSet
val names = namesMatches.map { s =>
    s.replaceAll("""\s""", " ").toLowerCase
  } map { name : String =>
    // Make pairs of words
    name.split(" ")
  }

// Going to need the lengths and term iterators
val lengths = index.getLengthsIterator
val allterms = (headlineTerms ++ quotes.flatten ++ names.flatten).toSet
val nodeMap = LinkedHashMap[Node, java.lang.Object]()
val termMap = LinkedHashMap[String, Node]()
// Gonna be naive here and just load extents for everyone - have to look at
// dependencies for that later
for (t <- allterms) {
  val n = formatNode(t, "extents")
  val iter = index.getIterator(n)
  termMap += (t -> n)
  nodeMap += (n -> iter)
}

// Make unigrams from the headline words, (dirichlet)
val headlineScorers = headlineTerms.map { t =>
  val scorer = dirichlet(collectionFrequency(termMap(t), index, lengths))
  val ps = ParameterizedScorer(scorer, lengths, nodeMap(termMap(t)))
  ps.weight = 1.2 // why not?
  ps
}
// unordered windows from the quotes, (JM)
val quoteScorers = quotes.map { listOfTerms =>
  val scorer = jm(collectionFrequency(termMap(listOfTerms(0)), index, lengths))
  val isect = uw(listOfTerms.size + 4)
  val iterators =
    listOfTerms.map(t => nodeMap(termMap(t)).asInstanceOf[TEI])
  val ps = ParameterizedScorer(scorer, isect, lengths, iterators)
  ps.weight = 0.35
  ps
}
// and od-1's of the names (BM25)
val nd = numDocuments(lengths).toDouble
val adl = avgLength(lengths)
val nameScorers = names.map { firstlast =>
  val idf = nd / (documentCount(termMap(firstlast(0)), index) + 0.5)
  val scorer = bm25(adl, idf)
  val isect = od(1)
  val iterators = firstlast.map(t => nodeMap(termMap(t)).asInstanceOf[TEI])
  val ps = ParameterizedScorer(scorer, isect, lengths, iterators.toList)
  ps.weight = 0.66
  ps
}

val allScorers = headlineScorers ++ quoteScorers ++ nameScorers
val iterators = nodeMap.values.toList.map(_.asInstanceOf[MovableIterator])
val initialResults = standardScoringLoop(allScorers, iterators, lengths, 1000)

// Have top 1K - now rerank using a smoothed KL-Divergence
// unfortunately - that means parsing with the lame TagTokenizer
val tokenizer = new TagTokenizer()
val docs = initialResults.map { sd =>
  val d = index.getDocument(index.getName(sd.docid), dummyParams)
  if (d.terms == null || d.terms.size == 0) tokenizer.tokenize(d)
  d // tokenize method returns void/Unit
}

// Now make probability distributions
val probDists = docs.map(multinomial(_))

// Have to do it w/ the ref document
val queryDist = multinomial(queryDoc).probs

// and now score those with KL-Divergence (and sort)
val cl = collectionLength(lengths)
val finalScoredDocs = probDists.map { candidate =>
  val keys = queryDist.keys
  val kldiv = keys.map { t =>
    val p = queryDist(t)
    val q = candidate.probs.getOrElse(t, 1.0 / cl)
    scala.math.log(p / q) * p
  }.sum
  ScoredDocument(candidate.id, kldiv)
}.toList.sorted(ScoredDocumentOrdering).take(100)

// Get doc names and print
printResults(finalScoredDocs, index)
