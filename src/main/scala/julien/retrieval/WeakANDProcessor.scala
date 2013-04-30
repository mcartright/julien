package julien
package retrieval

import scala.annotation.tailrec
import julien._

object WeakANDProcessor {
  def apply() = new WeakANDProcessor()
}

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
class WeakANDProcessor(factor: Double = 1.0) extends SimplePreloadingProcessor {
  override def finishScoring[T <: ScoredObject[T]](
    allSentinels: Seq[Sentinel],
    iterators: Set[GIterator],
    acc: Accumulator[T] = DefaultAccumulator[ScoredDocument]()
  ): List[T] = {
    val hackedAcc = acc.asInstanceOf[DefaultAccumulator[ScoredDocument]]

    // This is the WeakAND strategy - before scoring fully, we decide whether
    // this document has the potential to make it into the final list. If so,
    // it's fully scored.

    // Start with a full sort - the only one needed
    var sortedSentinels = allSentinels.toArray.sorted(SentinelOrdering)
    val scoreMinimum = sortedSentinels.map(_.feat.lowerBound).sum

    var running = true
    var threshold = hackedAcc.head.score * factor // Soft limit
    var lastScored = -1
    while (running) {
      val pivotPos = findPivot(sortedSentinels, scoreMinimum, threshold)
      if (pivotPos == -1 || sortedSentinels(pivotPos).iter.isDone) {
        // invariants not maintained - time to stop
        running = false
        debug("stopping")
      } else {
        val pivot = sortedSentinels(pivotPos).iter.currentCandidate
        debug(s"pivot = $pivot, lastScored = $lastScored")
        if (pivot <= lastScored) {
          // The pivot is old news. Move the pivot iterator past it
          // and try again
          val advancing = advancingSentinel(sortedSentinels, pivotPos, pivot)
          sortedSentinels(advancing).iter.movePast(pivot)
          shuffleDown(sortedSentinels, advancing)
        } else if (sortedSentinels.head.iter.currentCandidate == pivot) {
          // All iterators up to the pivot iterator are lined up. Score.
          iterators.foreach(_.syncTo(pivot))
          val score = sortedSentinels.map(_.feat.eval).sum
          // Pass muster, put it in the queue and update the threshold
          debug(s"adding ($pivot, $score)")
          hackedAcc += ScoredDocument(pivot, score)
          threshold = hackedAcc.head.score * factor
        } else {
          // Iterators in line before the pivot iterator are not lined
          // up yet - pick one and move it to the pivot.
          val advancing = advancingSentinel(sortedSentinels, pivotPos, pivot)
          sortedSentinels(advancing).iter.syncTo(pivot)
          shuffleDown(sortedSentinels, advancing)
        }
      }
    }

    // All done - return
    acc.result
  }

  // Assumption: limit is NOT negative infinity. We assume this b/c the method
  // is not called in the above code until after warming up the queue.
  def findPivot(s: Array[Sentinel], min: Double, limit: Double): Int = {
    // Small recursive function here
    @tailrec
    def sumif(s: Array[Sentinel], i: Int, sum: Double, lim: Double): Int = {
      if (i >= s.length) -1
      else if (sum > lim) i
      else if (s(i).iter.isDone) sumif(s, i+1, sum, lim)
      else sumif(s, i+1, sum+s(i).feat.upperBound, lim)
    }
    // aaaand go.
    sumif(s, 0, min, limit)
  }

  def advancingSentinel(s: Array[Sentinel], lim: Int, limDoc: Int): Int =
    s.view(0, lim).
      zipWithIndex.  // sentinel --> (sentinel, index in array)
      filter(_._1.iter.currentCandidate < limDoc).
      minBy(_._1.iter.totalEntries).
      _2 // extracts the position

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
