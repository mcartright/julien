package edu.umass.ciir.julien

import org.lemurproject.galago.core.util.ExtentArray
import org.lemurproject.galago.core.util.ExtentArrayIterator

object Intersections {
  type Intersection = (List[ExtentArray]) => Int

  // Intersection generators
  def od(width: Int = 1) : Intersection =
    (extents: List[ExtentArray]) => {
      val iterators = extents.map(new ExtentArrayIterator(_))
      var count = 0
      while (iterators.forall(_.isDone == false)) {
        // Make sure an iterator is after its preceding one
        iterators.reduceLeft { (i1, i2) =>
          while (i2.currentBegin < i1.currentEnd && !i2.isDone) i2.next
          i2
        }
        // Now see if we have a valid match
        val matched = iterators.sliding(2,1).map { P =>
          // Map pairs of iterators to booleans. All true = have match
          P(1).currentBegin - P(0).currentEnd < width
        }.reduceLeft((A,B) => A && B)
        if (matched) count += 1
        iterators(0).next
      }
      count
    }

  def uw(width: Int = 1) : Intersection =
    (extents: List[ExtentArray]) => {
      val iterators = extents.map(new ExtentArrayIterator(_))
      var count = 0
      while (iterators.forall(_.isDone == false)) {
        // Find bounds
        val minPos = iterators.map(_.currentBegin).min
        val maxPos = iterators.map(_.currentEnd).max

        // see if it fits
        if (maxPos - minPos <= width || width == -1) count += 1

        // move all lower bound iterators foward
        for (it <- iterators; if (it.currentBegin == minPos)) it.next
      }
      count
    }
}
