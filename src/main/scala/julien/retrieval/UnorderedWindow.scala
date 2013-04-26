package julien
package retrieval

import org.lemurproject.galago.core.util._

object UnorderedWindow {
  def apply(w: Int, t: PositionStatsView*) = new UnorderedWindow(w, t)
}

class UnorderedWindow(val width: Int, val terms: Seq[PositionStatsView])
    extends MultiTermView(terms) {
  // Again, being lazy about this number

  assume(terms.size > 1 && width >= terms.size, s"Window size must be >1 and at least as big as the number of iterators")

  override def updateStatistics = {
    super.updateStatistics
    statistics.collLength = terms.head.statistics.collLength
  }

  override def positions:  Positions = {
    val hits = Positions.newBuilder
    val iterators: Seq[BufferedIterator[Int]] = terms.map { t =>
      t.positions.iterator.buffered
    }
    while (iterators.forall(_.hasNext == true)) {
      val currentPositions = iterators.map(_.head)
      // Find bounds
      val minPos = currentPositions.min
      val maxPos = currentPositions.max

      // see if it fits
      if (maxPos - minPos <= width || width == -1) hits += minPos

      // move all lower bound iterators foward
      for (it <- iterators; if (it.head == minPos)) it.next
    }
    hits.result
  }

  override def isDense: Boolean = terms.forall(_.isDense)
  override def size: Int = statistics.docFreq.toInt
}
