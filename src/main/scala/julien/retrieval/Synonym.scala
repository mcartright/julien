package julien
package retrieval

import scala.collection.mutable.TreeSet
import galago.core.util.ExtentArray

object Synonym {
  def apply(terms: Term*) = new Synonym(terms)
}

class Synonym(terms: Seq[Term])
    extends MultiTermView(terms) {
  // Being lazy with the estimation of this number
  override def updateStatistics = {
    statistics.collLength = terms.head.attachedIndex.collectionLength
  }

  override def positions: ExtentArray = {
    // Assumption: a position is a one-to-one with a word, so the set union of
    // the position vectors of all involved terms is equal to the multiset union
    //
    // Based on that, a TreeSet is a sorted set implementation to hold results.
    // Boom.
//    val hits = TreeSet[Int]()
//    for (it <- terms.map(t => t.underlying.extents.begins)) {
//      hits ++= it
//    }
//    hits.toArray
    ExtentArray.empty
  }

  override def isDense: Boolean = terms.forall(_.isDense)
  override def size: Int = statistics.docFreq.toInt
}
