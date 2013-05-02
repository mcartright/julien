package julien
package retrieval

import scala.collection.BufferedIterator

object OrderedWindow {
  def apply(w: Int, t: PositionStatsView*) = new OrderedWindow(w, t)
}

class OrderedWindow(val width: Int, val terms: Seq[PositionStatsView])
  extends MultiTermView(terms) {
  // update the statistics object w/ our notion of "collection length"
  // We *could* say it's dependent on the size of the gram and width, but
  // that's a lot of work and no one else does it, so here's our lazy way out.
  // val adjustment = t.size * statistics.numDocs
  val adjustment = 0

  override def updateStatistics = {
    super.updateStatistics
    statistics.collLength =
      terms.head.statistics.collLength - adjustment
  }

  override def positions: Positions = {
    val hits = Positions.newBuilder
    val iterators: Seq[BufferedIterator[Int]] = terms.map {
      t =>
        t.positions.iterator.buffered
    }
    while (iterators.forall(_.hasNext)) {
      // Make sure an iterator is after its preceding one
      iterators.reduceLeft {
        (i1, i2) =>
          while (i2.hasNext && i2.head < i1.head) i2.next
          i2
      }
      if (iterators.exists(!_.hasNext)) return hits.result

      // Now see if we have a valid match; forall short circuits
      val matched = allInWidth(iterators, width)
      if (matched) {
        hits += iterators(0).head
      }
      iterators(0).next
    }
    hits.result
  }

  def allInWidth(iterators: Seq[BufferedIterator[Int]], width: Int): Boolean = {
    var idx = 0
    while(idx < iterators.size-1) {
      val left = iterators(idx)
      val right = iterators(idx+1)

      if(right.head - left.head <= width) {
        // good
        idx += 1
      } else return false
    }
    true
  }

  override def isDense: Boolean = terms.forall(_.isDense)

  override def size: Int = statistics.docFreq.toInt
}
