package julien
package retrieval

import scala.collection.BufferedIterator
import collection.mutable.ArrayBuffer
import galago.core.util.ExtentArray

object OrderedWindow {
  def apply(w: Int, t: PositionStatsView*) = new OrderedWindow(w, t)
}

class OrderedWindow(val width: Int, val terms: Seq[PositionStatsView])
  extends MultiTermView(terms) {
  assume(width > 0, s"OrderedWindow needs a positive width. Got $width")
  // update the statistics object w/ our notion of "collection length"
  // We *could* say it's dependent on the size of the gram and width, but
  // that's a lot of work and no one else does it, so here's our lazy way out.
  // val adjustment = t.size * statistics.numDocs
  val adjustment = 0

  override def updateStatistics(docid: InternalId) = {
    super.updateStatistics(docid)
    statistics.collLength =
      terms.head.statistics.collLength - adjustment
  }

  override def positions: ExtentArray = {
    val hits = new ExtentArray()
    val iterators: Seq[ExtentArray] = terms.map(_.positions)

    while (iterators(0).hasNext) {
      // if while advancing, we don't find a hit:
      if(!advance(iterators)) {
        return hits
      }

      // found a hit, keep going
      hits.add(iterators(0).head)
      iterators(0).next
    }
    hits
  }

  // returns true if a result has been found
  def advance(iterators: Seq[ExtentArray]): Boolean = {
    var idx = 0
    while(idx < iterators.size-1) {
      val left = iterators(idx)
      val right = iterators(idx+1)

      // make sure each iterator occurs in order
      while(right.hasNext && right.head < left.head) {
        right.next
      }

      // ran out of an iterator; return results immediately
      if(!right.hasNext)
        return false

      // if this iterator violates the window, restart loop
      if(right.head - left.head > width) {
        iterators(0).next
        if(!iterators(0).hasNext)
          return false
        idx = 0
      } else {
        idx += 1
      }
    }
    // found a hit that satisfies all our conditions
    true
  }

  override def isDense: Boolean = terms.forall(_.isDense)
  override def size: Int = statistics.docFreq.toInt
}
