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
    assume(terms.size > 1 && width >= terms.size, s"Window size must be >1 and at least as big as the number of iterators")

  // this is lazy because it needs to be attached to a data source aka "hooked up"
  // before underlying data source can be accessed.
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

  // how big??
  val hits =  new ExtentArray(10000)

  override def updateStatistics = {
    super.updateStatistics
  // Again, being lazy about this number
    statistics.collLength = terms.head.statistics.collLength
  }

  override def positions:  ExtentArray = {

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

  private final def moveMinForward(iterators: Array[ExtentArray], minPos : Int): Unit = {
    var j = 0
    while (j < iterators.length) {
      val cur = iterators(j).head
      if (cur == minPos) {
        iterators(j).next()
      }
      j += 1
    }
  }

  private final def allHaveNext(iterators: Array[ExtentArray]): Boolean = {
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

  private final def updateMinMaxPos(iterators: Array[ExtentArray]) = {
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

  override def isDense: Boolean = terms.forall(_.isDense)
  override def size: Int = statistics.docFreq.toInt
}
