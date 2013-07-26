package julien
package retrieval

import julien.Gram
import julien.eval.QueryResult
import julien.galago.core.parse.TagTokenizer
import julien.galago.tupleflow.Parameters
import scala.collection.mutable.{ListBuffer,PriorityQueue,HashMap,LinkedHashMap}

object RelevanceModel {

  // Get stopwords to filter
  val stopwords = Stopwords.inquery

  def apply[T <: ScoredObject](
    initial: QueryResult[T],
    fbDocs: Int = 10,
    fbTerms: Int = 10,
    filterTerms: Set[String] = Set[String]()
  )(implicit index: Index) : List[Gram] = {
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

    // histograms of the # of occurrences - each doc is a histogram
    val hists = docs.map( d => (d.identifier, d.histogram) ).toMap

    // Set of fb terms (keys of the histogram itself)
    var terms = hists.values.map(_.keySet).flatten
      .filterNot(stopwords(_))          // that are NOT stopwords
      .filterNot(_.size <= 1)           // that are NOT 1-character or less
      .filterNot(t => """\d+""".r == t) // and are NOT all digits
      .filterNot(filterTerms(_))        // and are NOT in the filter set
      .toSet

    // Apparently we need lengths too. Makes (docid -> length) map
    val doclengths = initialFactors.keys.map { docid =>
      (docid, index.length(docid))
    }.toMap

    // Time to score the terms
    val grams = terms.map { t =>
      // map to score-per-doc then sum
      val score = initialFactors.map { case (docid, sc) =>
        val tf =
          hists(docid).getOrElse(t, 0).toDouble / doclengths(docid)
        (sc * tf)
      }.sum
      val g = Gram(t, score)
      g
    }

    // Sort and keep top "fbTerms"
    grams.toList.sorted.take(fbTerms)
  }
}
