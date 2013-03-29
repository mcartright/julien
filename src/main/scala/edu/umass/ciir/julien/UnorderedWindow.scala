package edu.umass.ciir.julien

import org.lemurproject.galago.core.util.ExtentArray
import org.lemurproject.galago.core.util.ExtentArrayIterator

class UnorderedWindow(sources: List[BoundSource], val width: Int = 1)
    extends WindowSource(sources) {

  override def positions : ExtentArray = {
    val hits = new ExtentArray()
    val iterators = sources.map(s => new ExtentArrayIterator(s.positions))
    while (iterators.forall(_.isDone == false)) {
      // Find bounds
      val minPos = iterators.map(_.currentBegin).min
      val maxPos = iterators.map(_.currentEnd).max

      // see if it fits
      if (maxPos - minPos <= width || width == -1) hits.add(minPos, maxPos)

      // move all lower bound iterators foward
      for (it <- iterators; if (it.currentBegin == minPos)) it.next
    }
    hits
  }
}
