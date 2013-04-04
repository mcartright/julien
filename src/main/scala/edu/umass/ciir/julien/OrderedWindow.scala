package edu.umass.ciir.julien

import scala.collection.BufferedIterator

object OrderedWindow {
  def apply(w: Int, t: Term*) = new OrderedWindow(w, t)
}

class OrderedWindow(val width: Int, val terms: Seq[Term])
    extends MultiTermView(terms) {
  override def positions: Positions = {
    val hits = Positions.newBuilder
    val iterators : Seq[BufferedIterator[Int]] = terms.map(t =>
      Positions(t.underlying.extents).iterator.buffered)
    while (iterators.forall(_.hasNext)) {
      // Make sure an iterator is after its preceding one
      iterators.reduceLeft { (i1, i2) =>
        while (i2.hasNext && i2.head < i1.head) i2.next
        i2
      }
      if (iterators.exists(! _.hasNext)) return hits.result

      // Now see if we have a valid match
      val matched = iterators.sliding(2,1).map { P =>
        // Map pairs of iterators to booleans. All true = have match
        P(1).head - P(0).head < width
      }.reduceLeft((A,B) => A && B)
      if (matched) {
        hits += iterators.head.head
      }
      iterators(0).next
    }
    hits.result
  }
}
