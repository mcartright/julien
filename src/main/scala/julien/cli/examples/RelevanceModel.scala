package julien
package cli
package examples

import julien.retrieval._
import julien.retrieval.Utils._
import julien.retrieval.processor._
import julien.eval.QueryResult

import scala.collection.mutable.{ListBuffer,PriorityQueue,HashMap,LinkedHashMap}
import julien.galago.core.parse.{Tag,TagTokenizer}
import julien.galago.tupleflow.Parameters
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
    // Open an index - the implicit is to have it auto set for anything
    // that needs it.
    implicit val index : Index = Index.disk(params.getString("index"))

    // Set up to perform the first run
    val query = params.getString("query").split(" ").map(Term(_))
    val ql = Combine(query.map(a => Dirichlet(a, IndexLengths())))

    // run it and get results for the first run
    val results = QueryProcessor(ql).run()

    val selectedGrams = extractGrams(results, index)

    // Prep for adding to model
    val wrappedGrams = selectedGrams.map { gram =>
      Dirichlet(Term(gram.term), IndexLengths(), gram.score)
    }

    // Set weights using a single parameter (which ties them properly)
    val lambda = 0.7
    ql.weight = lambda
    val rm3 = Combine(
      List(ql, Combine(children = wrappedGrams, weight = (1-lambda)))
    )

    val finalResults = QueryProcessor(rm3).run()
    printResults(results, index, out)
  }

  def extractGrams(
    initial: QueryResult[ScoredDocument],
    index: Index
  ): List[Gram] = {
    val fbDocs = 10  // make proper variable later

    // take fbDocs
    var initialResults = initial.take(fbDocs)

    // now recover "probabilities" of each doc
    val max = initialResults.map(_.score).max
    val logSumExp = max + scala.math.log(initialResults.map { v =>
      scala.math.exp(v.score - max)
    }.sum)
    val initialFactors = initialResults.map { sd =>
      (sd.id, scala.math.exp(sd.score - logSumExp))
    }.toMap

    // get the actual documents, and count the grams
    val tokenizer = new TagTokenizer()
    val dummy = new Parameters()
    // load and tokenize docs if needed
    val docs = index.
      documents(initialFactors.keySet.toSeq).
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
    val doclengths = initialFactors.keys.map { docid =>
      (docid, index.length(docid))
    }.toMap

    // Time to score the terms
    val grams = terms.map { T =>
      // map to score-per-doc then sum
      val score = initialFactors.map { case (docid, score) =>
        val tf =
          hists(docid).getOrElse(T, 0).toDouble / doclengths(docid)
        (score * tf)
      }.sum
      Gram(T, score)
    }

    // Sort and keep top "fbTerms"
    val fbTerms = 20
    grams.toList.sorted.take(fbTerms)
  }
}
