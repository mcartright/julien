package julien
package retrieval

import scala.annotation.tailrec
import julien.galago.core.util._

object UnorderedWindow {
  def apply(w: Int, t: PositionStatsView*) = new UnorderedWindow(w, t)
}

class UnorderedWindow(val width: Int, val terms: Seq[PositionStatsView])
    extends MultiTermView(terms) {
  assume(terms.size > 1 && width >= terms.size,
    s"Window size must be >1 and at least as big as the number of iterators")

  override def updateStatistics = {
    super.updateStatistics
  // Again, being lazy about this number
    statistics.collLength = terms.head.statistics.collLength
  }

  override def positions:  Positions = {
    val hits = Positions.newBuilder
    val iterators: Array[BufferedIterator[Int]] = terms.map { t =>
      t.positions.iterator.buffered
    }.toArray
    while (iterators.forall(_.hasNext == true)) {
      // Find bounds
      //val currentPositions = iterators.map(_.head)
      //val minPos = currentPositions.min
      //val maxPos = currentPositions.max
      val minPos = min(iterators, 0)
      val maxPos = max(iterators, 0)

      // see if it fits
      if (maxPos - minPos < width || width == -1) hits += minPos

      // move all lower bound iterators foward
      //for (it <- iterators; if (it.head == minPos)) it.next
      movePast(iterators, 0, minPos)
    }
    hits.result
  }

  @tailrec
  private def movePast(
    its: Array[BufferedIterator[Int]],
    idx :Int,
    pos: Int): Unit =
    if (idx == its.length) return else {
      val it = its(idx)
      if (it.head == pos) it.next
      movePast(its, idx+1, pos)
    }

  @tailrec
  private def min(
    its: Array[BufferedIterator[Int]],
    idx: Int,
    m: Int = Int.MaxValue): Int = {
    if (idx == its.length) return m else {
      val newM = if (its(idx).head < m) its(idx).head else m
      min(its, idx+1, newM)
    }
  }

  @tailrec
  private def max(
    its: Array[BufferedIterator[Int]],
    idx: Int,
    m: Int = Int.MinValue): Int = {
    if (idx == its.length) return m else {
      val testM = its(idx).head
      val newM = if (testM > m) testM else m
      max(its, idx+1, newM)
    }
  }

  override def isDense: Boolean = terms.forall(_.isDense)
  override def size: Int = statistics.docFreq.toInt
}
