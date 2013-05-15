package julien
package retrieval

import scala.annotation.tailrec
import julien.galago.core.util._
import collection.mutable
import collection.mutable.ArrayBuffer

object UnorderedWindow {
  def apply(w: Int, t: PositionStatsView*) = new UnorderedWindow(w, t)
}

// width = -1 means that the whole document is considered a match
class UnorderedWindow(val width: Int, val terms: Seq[PositionStatsView])
    extends MultiTermView(terms) {
  assume(terms.size > 1, s"UnorderedWindow expects > 1 term")
  assume(width >= terms.size,
    s"width should be > # of iterators (got ${terms.length})")

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

  override def positions:  ExtentArray = {
    hits.clear
    reset(iterators)
    var break = false
    while (allHaveNext(iterators) && !break) {
      // But was refactored into the following faster code:
     // val (minPos, maxPos, minIdx) = updateMinMaxPos(iterators)

      var minIdx = -1
      var min = Int.MaxValue
      var max = 0
      var j = 0
      while (j < iterators.length) {
        val cur = iterators(j).head()
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

      // move all lower bound iterators foward
     // moveMinForward(iterators, minPos)
      if (iterators(minIdx).hasNext) {
        iterators(minIdx).next()
      } else {
        break = true
      }
    }
    hits
  }

  private def reset(iterators: Array[ExtentArray]): Unit = {
    var j = 0
    while (j < iterators.length) {
      iterators(j).reset()
      j += 1
    }
  }

  private def moveMinForward(
    iterators: Array[ExtentArray],
    minPos : Int
  ): Unit = {
    var j = 0
    while (j < iterators.length) {
      val cur = iterators(j).head
      if (cur == minPos) {
        iterators(j).next()
      }
      j += 1
    }
  }

  private def allHaveNext(iterators: Array[ExtentArray]): Boolean = {
    var j = 0
    while (j < iterators.length) {
      val hasNext = iterators(j).hasNext
      if (hasNext == false) {
        return false
      }
      j += 1
    }
    return true
  }

  private def updateMinMaxPos(iterators: Array[ExtentArray]) = {
    var minIdx = -1
    var min = Int.MaxValue
    var max = 0
    var j = 0
    while (j < iterators.length) {
      val cur = iterators(j).head()
      if (cur < min) {
        min = cur
        minIdx = j
      }
      if (cur > max) {
        max = cur
      }
      j += 1
    }
    (min, max, minIdx)
  }

  def walker = new Traversable[Posting] {
    val thePosting = new Posting(0, ExtentArray.empty)
    def foreach[U](f: Posting => U) {
      val movers = terms.flatMap(_.grab[Movable]).distinct
      if (movers.isEmpty) return
      val startPositions = movers.map(_.at)
      movers.foreach(_.reset)
      while (movers.exists(!_.isDone)) {
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
