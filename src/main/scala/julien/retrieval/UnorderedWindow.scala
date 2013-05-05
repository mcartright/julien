package julien
package retrieval

import scala.annotation.tailrec
import julien.galago.core.util._
import collection.mutable
import collection.mutable.ArrayBuffer

object UnorderedWindow {
  def apply(w: Int, t: PositionStatsView*) = new UnorderedWindow(w, t)
}

// width = -1 means that the whole document is considered a match
class UnorderedWindow(val width: Int, val terms: Seq[PositionStatsView])
    extends MultiTermView(terms) {
  assume(terms.size > 1 && width >= terms.size,
    s"Window size must be >1 and at least as big as the number of iterators")

  override def updateStatistics = {
    super.updateStatistics
  // Again, being lazy about this number
    statistics.collLength = terms.head.statistics.collLength
  }

  override def positions:  ExtentArray = {
    val hits = new ExtentArray()
    val iterators: Array[Positions] = terms.map {t =>
      Positions(t.positions)
    }.toArray

    while (iterators.forall(_.hasNext == true)) {
      // Find bounds

      //val currentPositions = iterators.map(_.head)
      //val minPos = currentPositions.min
      //val maxPos = currentPositions.max
      val (minPos, maxPos) = {
        var min = Int.MaxValue
        var max = Int.MinValue
        iterators.foreach(iter => {
          val cur = iter.head
          if(cur < min) min = cur
          if(cur > max) max = cur
        })
        (min, max)
      }

      // see if it fits
      if (maxPos - minPos < width || width == -1) hits.add(minPos)

      // move all lower bound iterators foward
      for (it <- iterators; if (it.head == minPos)) it.next
      movePast(iterators, 0, minPos)
    }
    hits
  }

  @tailrec
  private def movePast(
    its: Array[Positions],
    idx :Int,
    pos: Int): Unit =
    if (idx == its.length) return else {
      val it = its(idx)
      if (it.head == pos) it.next
      movePast(its, idx+1, pos)
    }

  override def isDense: Boolean = terms.forall(_.isDense)
  override def size: Int = statistics.docFreq.toInt
}
