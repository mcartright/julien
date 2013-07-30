package julien
package retrieval

import scala.annotation.tailrec
import julien.galago.core.util._
import collection.mutable
import collection.mutable.ArrayBuffer
import julien.behavior._

object UnorderedWindow {
  /** Attempts to provide the fastest implementation of the unordered
    * window view as possible, based on the provided PositionStatsViews.
    */
  def apply(w: Int, t: PositionStatsView*) =
    if (t.forall(_.isInstanceOf[PositionsBufferView]))
      new BufferedUnorderedWindow(w, t)
    else
      new UnorderedWindow(w, t)

  def threadsafe(w: Int, t: PositionStatsView*) = new UnorderedWindow(w, t)
}

class UnorderedWindow(val width: Int, val terms: Seq[PositionStatsView])
    extends MultiTermView(terms) {
  assume(terms.size > 1, "UnorderedWindow expects > 1 term")
  assume(width >= terms.size,
    s"width should be > # of iterators (got ${terms.length})")

  def positions(id: Int): ExtentArray = {
    val hits = new ExtentArray()
    val eArrays = terms.map { t =>
      t.synchronized(t.positions(id).copy)
    }.toArray
    if (eArrays.exists(!_.hasNext)) return hits
    var break = false
    while (allHaveNext(eArrays) && !break) {
      // val (minPos, maxPos, minIdx) = updateMinMaxPos(eArrays)
      // was refactored into the following faster code:
      var minIdx = -1
      var min = Int.MaxValue
      var max = 0
      var j = 0
      while (j < eArrays.length) {
        val cur = eArrays(j).head()
        if (cur < min) {
          min = cur
          minIdx = j
        }
        if (cur > max) {
          max = cur
        }
        j += 1
      }

      // see if it fits
      if (max - min < width || width == -1) hits.add(min, max)

      // move all lower bound eArrays foward
     // moveMinForward(eArrays, minPos)
      if (eArrays(minIdx).hasNext) {
        eArrays(minIdx).next()
      } else {
        break = true
      }
    }
    hits
  }

  protected def allHaveNext(eArrays: Array[ExtentArray]): Boolean = {
    var j = 0
    while (j < eArrays.length) {
      val hasNext = eArrays(j).hasNext
      if (hasNext == false) {
        return false
      }
      j += 1
    }
    return true
  }

  def walker = new Traversable[Posting] {
    val thePosting = new Posting(0, ExtentArray.empty)
    def foreach[U](f: Posting => U) {
      val movers = terms.flatMap(_.grab[Movable]).distinct
      if (movers.isEmpty) return
      val startPositions = movers.map(_.at)
      movers.foreach(_.reset)
      var candidate = movers.map(_.at).max
      while (movers.forall(!_.isDone)) {
        var candidate = movers.map(_.at).max
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

// width = -1 means that the whole document is considered a match
class BufferedUnorderedWindow(
  val w: Int,
  val t: Seq[PositionStatsView]
) extends UnorderedWindow(w, t) {
  private val hits: ExtentArray = new ExtentArray(10000)

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

  override def positions(id: Int):  ExtentArray = {
    hits.clear
    if (!ensurePosition(id)) return hits

    reset(eArrays)
    var break = false
    while (allHaveNext(eArrays) && !break) {
      // val (minPos, maxPos, minIdx) = updateMinMaxPos(eArrays)
      // was refactored into the following faster code:
      var minIdx = -1
      var min = Int.MaxValue
      var max = 0
      var j = 0
      while (j < eArrays.length) {
        val cur = eArrays(j).head()
        if (cur < min) {
          min = cur
          minIdx = j
        }
        if (cur > max) {
          max = cur
        }
        j += 1
      }

      // see if it fits
      if (max - min < width || width == -1) hits.add(min, max)

      // move all lower bound eArrays foward
     // moveMinForward(eArrays, minPos)
      if (eArrays(minIdx).hasNext) {
        eArrays(minIdx).next()
      } else {
        break = true
      }
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
