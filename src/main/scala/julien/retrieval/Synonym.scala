package julien
package retrieval

import scala.collection.mutable.ArrayBuffer
import galago.core.util.ExtentArray
import julien.behavior._

object Synonym {
  def apply(terms: PositionStatsView*) = new Synonym(terms)
}

class Synonym(terms: Seq[PositionStatsView])
    extends MultiTermView(terms) {
  assume(terms.size > 1, "Synonym expects > 1 term")

  override def positions(id: InternalId): ExtentArray = {
    val hits = new ExtentArray()
    val eArrays = terms.map { t =>
      t.synchronized(t.positions(id).copy)
    }.toArray
    val b = ArrayBuffer[Int]()
    for (it <- eArrays) {
      it.reset
      while(it.hasNext) b += it.next
    }

    for (p <- b.sorted) hits.add(p)
    hits
  }

  def walker = new Traversable[Posting] {
    val thePosting = new Posting(0, ExtentArray.empty)
    def foreach[U](f: Posting => U) {
      val movers = terms.flatMap(_.grab[Movable]).distinct
      if (movers.isEmpty) return
      val startPositions = movers.map(_.at)
      movers.foreach(_.reset)
      var candidate = movers.map(_.at).min
      while (movers.exists(!_.isDone)) {
        val p = positions(candidate)
        thePosting.docid = candidate
        thePosting.positions = p
        f(thePosting)
        candidate = movers.map(_.movePast(candidate)).min
      }

      // Done iterating - now move to the right positions
      movers.foreach(_.reset)
      for ((m, p) <- movers.zip(startPositions)) m.moveTo(p)
    }
  }
}
