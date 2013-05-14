package julien
package retrieval

import collection.Traversable
import collection.mutable.ArrayBuffer
import galago.core.util.ExtentArray

object OrderedWindow {
  def apply(w: Int, t: PositionStatsView*) = new OrderedWindow(w, t)
}

class OrderedWindow(val width: Int, val terms: Seq[PositionStatsView])
  extends MultiTermView(terms) {
  assume(width > 0, s"OrderedWindow needs a positive width. Got $width")

  lazy val iterators: Array[ExtentArray] = {
    val itBuffer = Array.newBuilder[ExtentArray]
    var t = 0
    val numTerms =  terms.size
    while (t < numTerms) {
      itBuffer += terms(t).positions
      t += 1
    }
    itBuffer.result()
  }

  override def positions: ExtentArray = {
    hits.clear
    reset(iterators)
    while (iterators(0).hasNext) {
      // if while advancing, we don't find a hit:
      if(!advance(iterators)) {
        return hits
      }

      // found a hit, keep going
      hits.add(iterators(0).head)
      iterators(0).next
    }
    hits
  }

  // returns true if a result has been found
  def advance(iterators: Array[ExtentArray]): Boolean = {
    var idx = 0
    while(idx < iterators.length-1) {
      val left = iterators(idx)
      val right = iterators(idx+1)

      // make sure each iterator occurs in order
      while(right.hasNext && right.head < left.head) {
        right.next
      }

      // ran out of an iterator; return results immediately
      if(!right.hasNext) {
        return false
      }

      // if this iterator violates the window, restart loop
      if(right.head - left.head > width) {
        iterators(0).next
        if(!iterators(0).hasNext)
          return false
        idx = 0
      } else {
        idx += 1
      }
    }
    // found a hit that satisfies all our conditions
    true
  }

  private def reset(iterators: Array[ExtentArray]): Unit = {
    var j = 0
    while (j < iterators.length) {
      iterators(j).reset()
      j += 1
    }
  }

  // Traversable works better here because we can mark the locations of
  // the underlying movers, reset, iterate over all of them, and then
  // go back to where we started - all encapsulated in this one function.
  def walker = new Traversable[Posting] {
    val thePosting = new Posting(0, ExtentArray.empty)
    def foreach[U](f: Posting => U) {
      val movers = terms.flatMap(_.grab[Movable]).distinct
      if (movers.isEmpty) return
      val startPositions = movers.map(_.at)
      movers.foreach(_.reset)
      while (movers.forall(!_.isDone)) {
        val candidate = movers.map(_.at).max
        movers.foreach(_.moveTo(candidate))
        if (movers.forall(_.matches(candidate))) {
          val p = positions
          thePosting.docid = candidate
          thePosting.positions = p
          f(thePosting)
        }
        movers.foreach(_.movePast(candidate))
      }

      // Done iterating - now move to the right positions
      movers.foreach(_.reset)
      for ((m, p) <- movers.zip(startPositions)) m.moveTo(p)
    }
  }
}
