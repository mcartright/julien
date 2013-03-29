package edu.umass.ciir.julien

import org.lemurproject.galago.core.util.ExtentArray
import org.lemurproject.galago.core.util.ExtentArrayIterator

class OrderedWindow(sources: List[BoundSource], val width: Int = 1)
    extends WindowSource(sources) {

  def positions: ExtentArray = {
    val hits = new ExtentArray()
    val iterators = sources.map(s => new ExtentArrayIterator(s.positions))
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
      if (matched) {
        hits.add(iterators.head.currentBegin, iterators.last.currentEnd)
      }
      iterators(0).next
    }
    hits
  }
}
