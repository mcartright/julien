package julien
package retrieval

import scala.collection.mutable.ArrayBuffer
import galago.core.util.ExtentArray

object Synonym {
  def apply(terms: Term*) = new Synonym(terms)
}

class Synonym(terms: Seq[Term])
    extends MultiTermView(terms) {

  lazy val iterators: Array[ExtentArray] = {
    val itBuffer = Array.newBuilder[ExtentArray]
    var t = 0
    val numTerms =  terms.size
    while (t < numTerms) {
      itBuffer += terms(t).positionsBuffer
      t += 1
    }
    itBuffer.result()
  }

  private val hits: ExtentArray = new ExtentArray()

  override def positions(id: InternalId): ExtentArray = {
    hits.clear
    if (!ensurePosition(id)) return hits
    val b = ArrayBuffer[Int]()
    for (it <- iterators) {
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
