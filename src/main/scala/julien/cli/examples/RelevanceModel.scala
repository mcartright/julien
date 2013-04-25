package julien
package cli
package examples

import julien.retrieval._
import julien.retrieval.Utils._

import scala.collection.mutable.{ListBuffer,PriorityQueue,HashMap,LinkedHashMap}
import org.lemurproject.galago.core.parse.{Tag,TagTokenizer}
import org.lemurproject.galago.tupleflow.Parameters
import scala.collection.JavaConversions._
import scala.collection.immutable.Set
import scala.collection.Map

import java.io.PrintStream

object RelevanceModel extends Example {
  lazy val name: String = "relmodel"

  def checksOut(p: Parameters): Boolean =
    (p.containsKey("query") && p.containsKey("index"))

  val help: String = """
Executes the RM3 variant of the Relevance Model.
Required parameters:

    query        string form of desired query
    index        location existing index
"""



  def run(params: Parameters, out: PrintStream): Unit = {
    // Set up to perform the first run
    val query = params.getString("query").split(" ").map(Term(_))
    val ql = Combine(query.map(a => Dirichlet(a, IndexLengths())))

    // Open an index
    val index : Index = Index.disk(params.getString("index"))

    // Make a processor to run it
    val processor = SimpleProcessor()

    // Attach the query model to the index
    ql.hooks.foreach(_.attach(index))

    // Add the model to the processor
    processor.add(ql)

    // run it and get results for the first run
    val results = processor.run

    val selectedGrams = extractGrams(results, index)

    // Prep for adding to model
    val wrappedGrams = selectedGrams.map { gram =>
      Weight(Dirichlet(Term(gram.term), IndexLengths()), gram.score)
    }

    val rm3 = Combine(
      List[FeatureOp](Weight(ql, 0.7),
      Weight(Combine(wrappedGrams), 0.3))
    )

    processor.clear
    rm3.hooks.foreach(_.attach(index))
    processor.add(rm3)

    val finalResults = processor.run
    printResults(results, index, out)
  }

  def extractGrams(initial: List[ScoredDocument], index: Index): List[Gram] = {
    val fbDocs = 10  // make proper variable later

    // take fbDocs
    var initialResults = initial.take(fbDocs)

    // now recover "probabilities" of each doc
    val max = initialResults.map(_.score).max
    val logSumExp = max + scala.math.log(initialResults.map { v =>
      scala.math.exp(v.score - max)
    }.sum)
    initialResults = initialResults.map { sd =>
      ScoredDocument(sd.docid, scala.math.exp(sd.score - logSumExp))
    }

    // get the actual documents, and count the grams
    val tokenizer = new TagTokenizer()
    val dummy = new Parameters()
    // load and tokenize docs if needed
    val docs = index.
      documents(initialResults.map(_.docid)).
      map { d =>
        if (!d.hasTermVector)
          Document(tokenizer.tokenize(d.content))
        else
          d
      }

    // Get stopwords to filter
    val stopwords = Stopwords.inquery

    // histograms of the # of occurrences - each doc is a histogram
    val hists = docs.map( d => (d.identifier, d.histogram) ).toMap

    // Set of fb terms (keys of the histogram itself)
    var terms = hists.values.map(_.keySet).flatten
      .filterNot(stopwords(_))          // that are NOT stopwords
      .filterNot(_.size <= 1)           // that are NOT 1-character or less
      .filterNot(t => """\d+""".r == t) // and are NOT all digits

    // Apparently we need lengths too. Makes (docid -> length) map
    val doclengths = initialResults.map { doc =>
      (doc.docid, index.length(doc.docid))
    }.toMap

    // Time to score the terms
    val grams = terms.map { T =>
      // map to score-per-doc then sum
      val score = initialResults.map { SD =>
        val tf =
          hists(SD.docid).getOrElse(T, 0).toDouble / doclengths(SD.docid)
        (SD.score * tf)
      }.sum
      Gram(T, score)
    }

    // Sort and keep top "fbTerms"
    val fbTerms = 20
    grams.toList.sorted(GramOrdering).take(fbTerms)
  }
}
