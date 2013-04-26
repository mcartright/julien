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

      // Now see if we have a valid match
      iterators.foreach(s => println(s.head))
      val matched = iterators.sliding(2, 1).map {
        P =>
        // Map pairs of iterators to booleans. All true = have match
          P(1).head - P(0).head <= width
      }.reduceLeft((A, B) => A && B)
      if (matched) {
        hits += iterators.head.head
      }
      iterators(0).next
    }
    hits.result
  }

  override def isDense: Boolean = terms.forall(_.isDense)

  override def size: Int = statistics.docFreq.toInt
}
