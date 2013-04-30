package julien
package retrieval

import scala.collection.mutable.PriorityQueue
import scala.annotation.tailrec
import julien._

object MaxscoreProcessor {
  def apply() = new MaxscoreProcessor()
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
class MaxscoreProcessor extends SimplePreloadingProcessor {
  override def finishScoring[T <: ScoredObject[T]](
    allSentinels: Seq[Sentinel],
    activeSentinels: Seq[Sentinel],
    acc: Accumulator[T] = DefaultAccumulator[ScoredDocument]()
  ): List[T] = {
    val hackedAcc = acc.asInstanceOf[DefaultAccumulator[ScoredDocument]]
    var unfinished = activeSentinels
    // Build the sentinel list - sort is on idf
    val sentinels = allSentinels.sortBy(_.iter.totalEntries)

    // establish upper bound of all features - every document starts at
    // this value and is progressively lowered to the true score
    val startingScore = sentinels.map(_.feat.upperBound).sum
    // direct access to the underlying iterators
    val iterators = _models.flatMap(m => m.iHooks.map(_.underlying)).toSet

    // Now start looking for cutoffs
    var threshold = hackedAcc.head.score
    var sidx = getSentinelIndex(sentinels, 0, threshold, startingScore)

    // HACK - would like this to be cleaner
    val lengths = iterators.filter(_.isInstanceOf[LI]).head

    // Scala does not natively support a "break" construct, so let's avoid it.
    // We simply need to set the candidate once before entering the while
    // loop, then check at the end of the loop
    var candidate = getMinCandidate(unfinished)
    while (candidate < Int.MaxValue) {
      val drivers = sentinels.take(sidx)
      if (drivers.exists(_.iter.hasMatch(candidate))) {
        drivers.foreach(_.iter.syncTo(candidate))
        lengths.syncTo(candidate)
        // Sum the active sentinels
        val senscore = drivers.map(_.feat.eval).sum

        // Now add the rest of them until we're done or it's below threshold
        // doing this via tail recursion
        val (score, pos) =
          conditionalAddSentinel(sentinels, sidx, threshold, senscore)

        if (pos == sentinels.size && score > threshold) {
          hackedAcc += ScoredDocument(candidate, score)
          threshold = hackedAcc.head.score
          sidx = getSentinelIndex(sentinels, 0, threshold, startingScore)
        }
      }
      unfinished.foreach(_.iter.movePast(candidate))

      // Grab next candidate
      unfinished = unfinished.filterNot(_.iter.isDone)
      candidate = getMinCandidate(unfinished)
    }

    // All done - return
    acc.result
  }

    // Helper functions that are internally defined
  @tailrec
  private def conditionalAddSentinel(
    sents: Seq[Sentinel],
    idx: Int,
    threshold: Double,
    oldScore: Double): Tuple2[Double, Int] =
    // base case - either we're out of sentinels,
    // or we dropped below the threshold
    if (idx == sents.size || oldScore < threshold)
      (oldScore, idx)
    else
      // Add one more sentinel to the total
      conditionalAddSentinel(sents, idx+1, threshold,
        oldScore + sents(idx).feat.eval - sents(idx).dec)

  @tailrec
  private def getSentinelIndex(
    sents: Seq[Sentinel],
    idx: Int,
    threshold: Double,
    currentScore: Double): Int =
    if (idx == sents.size || currentScore < threshold)
      return idx
    else
      getSentinelIndex(sents, idx+1, threshold, currentScore - sents(idx).dec)
}
