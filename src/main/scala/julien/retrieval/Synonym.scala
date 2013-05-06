package julien
package retrieval

import scala.collection.mutable.ArrayBuffer
import galago.core.util.ExtentArray

object Synonym {
  def apply(terms: Term*) = new Synonym(terms)
}

class Synonym(terms: Seq[Term])
    extends MultiTermView(terms) {


  // Being lazy with the estimation of this number
  override def updateStatistics(docid: Docid) = {
    statistics.collLength = terms.head.attachedIndex.collectionLength
  }

  override def positions: ExtentArray = {
    val arrays = terms.map(_.underlying.extents)
    val b = ArrayBuffer[Int]()
    for (a <- arrays) {
      while (a.hasNext) b += a.next
    }

    val hits = new ExtentArray()
    for (p <- b.sorted) hits.add(p)
    hits
  }

  override def isDense: Boolean = terms.forall(_.isDense)
  override def size: Int = statistics.docFreq.toInt
}
