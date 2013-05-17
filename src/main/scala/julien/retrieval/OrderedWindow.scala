package julien
package retrieval

import collection.Traversable
import collection.mutable.ArrayBuffer
import galago.core.util.ExtentArray

object OrderedWindow {
  /** Attempts to provide the fastest implementation of the ordered window
    * view as possible, based on the provided PositionStatsViews
    */
  def apply(w: Int, t: PositionStatsView*) =
    if (t.forall(_.isInstanceOf[PositionsBufferView]))
      new BufferedOrderedWindow(w, t)
    else
      new OrderedWindow(w, t)

  def threadsafe(w: Int, t: PositionStatsView*) = new OrderedWindow(w, t)
}

/** Standard OrderedWindow implementation. Not the fastest possible, but this
  * version is thread-safe.
  */
class OrderedWindow(
  val width: Int,
  val terms: Seq[PositionStatsView]
) extends MultiTermView(terms) {
  assume(terms.size > 1, s"OrderedWindow expects > 1 term.")
  assume(width > 0, s"OrderedWindow needs a positive width. Got $width")

  def positions(id: InternalId): ExtentArray = {
    val hits = new ExtentArray()

    // Make local copies to be safe
    val eArrays =
      terms.map { t => t.synchronized(t.positions(id).copy) }.toArray
    if (eArrays.exists(!_.hasNext)) return hits
    // Copied below, but a bit faster to inline
    while (eArrays(0).hasNext) {
      // if while advancing, we don't find a hit:
      if(!advance(eArrays)) {
        return hits
      }

      // found a hit, keep going
      hits.add(eArrays(0).head)
      eArrays(0).next
    }
    hits
  }

  // returns true if a result has been found
  protected def advance(eArrays: Array[ExtentArray]): Boolean = {
    var idx = 0
    while(idx < eArrays.length-1) {
      val left = eArrays(idx)
      val right = eArrays(idx+1)

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
        eArrays(0).next
        if(!eArrays(0).hasNext)
          return false
        idx = 0
      } else {
        idx += 1
      }
    }
    // found a hit that satisfies all our conditions
    true
  }

  /** Traversable works better here because we can mark the locations of
    *  the underlying movers, reset, iterate over all of them, and then
    *  go back to where we started - all encapsulated in this one function.
    *
    * More importantly, the user is not given movement control - they can
    * only act on the viewed postings as they are traversed.
    */
  def walker = new Traversable[Posting] {
    val thePosting = new Posting(0, ExtentArray.empty)
    def foreach[U](f: Posting => U) {
      val movers = terms.flatMap(_.grab[Movable]).distinct
      if (movers.isEmpty) return
      val startPositions = movers.map(_.at)
      movers.foreach(_.reset)
      var candidate = movers.map(_.at).max
      while (movers.forall(!_.isDone)) {
        if (movers.forall(_.moveTo(candidate))) {
          val p = positions(candidate)
          thePosting.docid = candidate
          thePosting.positions = p
          f(thePosting)
        }
        candidate = movers.map(_.movePast(candidate)).max
      }

      // Done iterating - now move to the right positions
      movers.foreach(_.reset)
      for ((m, p) <- movers.zip(startPositions)) m.moveTo(p)
    }
  }
}

/** Optimized version of the [[julien.retrieval.OrderedWindow]].
  * Uses cached extent array references to intersect positions
  * faster. Not thread-safe.
  */
final class BufferedOrderedWindow(val w: Int, val t: Seq[PositionStatsView])
  extends OrderedWindow(w, t) {
  lazy val eArrays: Array[ExtentArray] = {
    val itBuffer = Array.newBuilder[ExtentArray]
    var t = 0
    val numTerms =  terms.size
    while (t < numTerms) {
      itBuffer += terms(t).asInstanceOf[PositionsBufferView].positionsBuffer
      t += 1
    }
    itBuffer.result()
  }

  // Only use one copy - much faster
  private val hits: ExtentArray = new ExtentArray(10000)

  /** Optimized version of this method: makes use of cached extent buffers
    * to reduce the number of internal method calls and object creation.
    */
  override def positions(id: InternalId): ExtentArray = {
    hits.clear
    // Ensure lined up, or return empty
    if (!ensurePosition(id)) return hits
    reset(eArrays)
    while (eArrays(0).hasNext) {
      // if while advancing, we don't find a hit:
      if(!advance(eArrays)) {
        return hits
      }

      // found a hit, keep going
      hits.add(eArrays(0).head)
      eArrays(0).next
    }
    hits
  }

  private def reset(eArrays: Array[ExtentArray]): Unit = {
    var j = 0
    while (j < eArrays.length) {
      eArrays(j).reset()
      j += 1
    }
  }
}
