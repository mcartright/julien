package julien
package retrieval
package processor

import scala.annotation.tailrec
import scala.math._
import julien._
import julien.behavior._

/** This processor handles bag-of-words queries that contain
  * features that have actual bounds (vs infinite bounds).
  * Does a structural check for a BOW model, and if it fits,
  * removes the top-level combine and runs the array of
  * features below it. Notice this only has a 1-level view of
  * the situation. If the next-level down features are complex
  * i.e. use separate indexes or multiple term sources, it'll do
  * it's best with what it has.
  *
  * This model exists for pedagogical purposes more than being
  * recent state-of-the-art.
  *
  * Assumptions:
  *    - No complex views are used (technically means that features
  *      with more than 1 sparse view are disallowed)
  *    - Only 1 index is being used
  *    - Query is a bag of words (term independence assumption)
  */
class WeakANDProcessor[T <: ScoredObject[T]] private[processor] (
  root: Feature,
  acc: Accumulator[T],
  factor: Double = 1.0
)
    extends SimplePreloadingProcessor[T](root, acc)
{
  override def finishScoring(
    allSentinels: Array[Sentinel],
    iterators: Array[Movable],
    acc: Accumulator[T]
  ): List[T] = {
    val hackedAcc = acc.asInstanceOf[DefaultAccumulator[ScoredDocument]]

    // This is the WeakAND strategy - before scoring fully, we decide whether
    // this document has the potential to make it into the final list. If so,
    // it's fully scored.

    // Start with a full sort - the only one needed
    var sortedSentinels = allSentinels.sorted(SentinelOrdering)
    var scoreMinimum = 0.0
    var i = 0
    while (i < sortedSentinels.length) {
      scoreMinimum += sortedSentinels(i).feat.lowerBound
    }

    var running = true
    var threshold = hackedAcc.head.score * factor // Soft limit
    var lastScored = -1
    while (running) {
      val pivotPos = findPivot(sortedSentinels, scoreMinimum, threshold)
      if (pivotPos == -1 || sortedSentinels(pivotPos).iter.isDone) {
        // invariants not maintained - time to stop
        running = false
      } else {
        val pivot = sortedSentinels(pivotPos).iter.at
        if (pivot <= lastScored) {
          // The pivot is old news. Move the pivot iterator past it
          // and try again
          val advancing = advancingSentinel(sortedSentinels, pivotPos, pivot)
          sortedSentinels(advancing).iter.movePast(pivot)
          shuffleDown(sortedSentinels, advancing)
        } else if (sortedSentinels.head.iter.at == pivot) {
          // Two gross but tight/fast loops in here

          // All iterators up to the pivot iterator are lined up. Score.
          var i = 0
          while (i < iterators.length) {
            iterators(i).moveTo(pivot)
            i += 1
          }

          // Slow but pretty
          // val score = sortedSentinels.map(_.feat.eval).sum
          // vs. fast but blah
          i = 0
          var score = 0.0
          while (i < sortedSentinels.length) {
            score += sortedSentinels(i).feat.eval(pivot)
            i += 1
          }

          // Pass muster, put it in the queue and update the threshold
          hackedAcc += ScoredDocument(pivot, score)
          threshold = hackedAcc.head.score * factor
          lastScored = pivot
        } else {
          // Iterators in line before the pivot iterator are not lined
          // up yet - pick one and move it to the pivot.
          val advancing = advancingSentinel(sortedSentinels, pivotPos, pivot)
          sortedSentinels(advancing).iter.moveTo(pivot)
          shuffleDown(sortedSentinels, advancing)
        }
      }
    }

    // All done - return
    acc.result
  }

  // Assumption: limit is NOT negative infinity. We assume this b/c the method
  // is not called in the above code until after warming up the queue.
  @tailrec
  final def findPivot(
    s: Array[Sentinel],
    sum: Double,
    limit: Double,
    i: Int = 0): Int = {
    if (i >= s.length) return -1
    else {
      val newSum = if (s(i).iter.isDone)
        sum
      else
        sum + (s(i).feat.upperBound - s(i).feat.lowerBound)
      if (newSum > limit) i
      else findPivot(s, newSum, limit, i+1)
    }
  }

  @tailrec
  final def advancingSentinel(
    s: Array[Sentinel],
    lim: Int,
    limDoc: Int,
    idx: Int = 0,
    midx: Int = 0,
    m: Long = Long.MaxValue): Int = {
    if (idx >= lim) midx // need (best min, pos)
    else if (s(idx).iter.at >= limDoc)
      advancingSentinel(s, lim, limDoc, idx+1, midx, m)
    else {
      val testMin = s(idx).iter.size
      if (testMin < m)
        advancingSentinel(s, lim, limDoc, idx+1, idx, testMin)
      else
        advancingSentinel(s, lim, limDoc, idx+1, midx, m)
    }
  }

  def shuffleDown(s: Array[Sentinel], start: Int): Unit = {
    var i = start
    while (i < s.length-1) {
      val result = SentinelOrdering.compare(s(i), s(i+1))
      if (result <= 0) return // indicates all done
      val tmp = s(i)
      s(i) = s(i+1)
      s(i+1) = tmp
      i += 1
    }
  }
}
