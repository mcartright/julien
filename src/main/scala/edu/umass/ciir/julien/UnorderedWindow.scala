package edu.umass.ciir.julien

import org.lemurproject.galago.core.util._

object UnorderedWindow {
  def apply(w: Int, t: Term*) = new UnorderedWindow(w, t)
}

class UnorderedWindow(val width: Int, val terms: Seq[Term])
    extends MultiTermView(terms) {
  override def positions:  Positions = {
    val hits = Positions.newBuilder
    val iterators: Seq[BufferedIterator[Int]] = terms.map(t =>
      Positions(t.underlying.extents).iterator.buffered)
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
}
