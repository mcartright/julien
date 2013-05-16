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
    allSentinels: Array[Sentinel],
    iterators: Array[Movable],
    acc: Accumulator[T] = DefaultAccumulator[ScoredDocument]()
  ): List[T] = {
    val hackedAcc = acc.asInstanceOf[DefaultAccumulator[ScoredDocument]]

    // Build the sentinel list - sort is on idf
    val sentinels = allSentinels.sortBy(_.iter.size)

    // establish upper bound of all features - every document starts at
    // this value and is progressively lowered to the true score
    val startingScore = sentinels.foldLeft(0.0) { (sum, sen) =>
      sum + sen.feat.upperBound
    }

    // Now start looking for cutoffs
    var threshold = hackedAcc.head.score
    var sidx = getSentinelIndex(sentinels, 0, threshold, startingScore)

    // Has to sit outside the sentinels...because, for now.
    val lengths = getLengthsIterator(iterators)

    // Scala does not natively support a "break" construct, so let's avoid it.
    // We simply need to set the candidate once before entering the while
    // loop, then check at the end of the loop
    var selected = sentinels.take(sidx)
    var drivers = selected.map(_.iter).filterNot(_.isDone)
    var candidate = getMinCandidate(drivers)
    while (candidate < Int.MaxValue) {
      if (drivers.exists(_.matches(candidate))) {
        // line up lengths
        lengths.moveTo(candidate)

        // Sum the active sentinels
        val senscore = selected.foldLeft(startingScore) { (score, sent) =>
          sent.iter.moveTo(candidate)
          val r = score + (sent.feat.eval - sent.feat.upperBound)
          r
        }
        // Now add the rest of them until we're done or it's below threshold
        // doing this via tail recursion
        val (score, pos) =
          conditionalAddSentinel(sentinels,
            candidate, sidx, threshold, senscore)
        if (pos == sentinels.size && score > threshold) {
          hackedAcc += ScoredDocument(candidate, score)
          if (threshold != hackedAcc.head.score) {
            threshold = hackedAcc.head.score
            sidx = getSentinelIndex(sentinels, 0, threshold, startingScore)
            selected = sentinels.take(sidx)
            drivers = selected.map(_.iter)
          }
        }
      }

      var i = 0
      while (i < drivers.length) {
        drivers(i).movePast(candidate)
        i += 1
      }

      drivers = drivers.filterNot(_.isDone)

      // Grab next candidate
      candidate = getMinCandidate(drivers)
    }

    // All done - return
    acc.result
  }

    // Helper functions that are internally defined
  @tailrec
  private def conditionalAddSentinel(
    sents: Seq[Sentinel],
    candidate: Int,
    idx: Int,
    threshold: Double,
    oldScore: Double): Tuple2[Double, Int] =
    // base case - either we're out of sentinels,
    // or we dropped below the threshold
    if (idx == sents.size || oldScore < threshold)
      (oldScore, idx)
    else {
      // Add one more sentinel to the total
      sents(idx).iter.moveTo(candidate)
      val r = oldScore + (sents(idx).feat.eval - sents(idx).feat.upperBound)
      conditionalAddSentinel(sents, candidate, idx+1, threshold, r)
    }

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

  // Assumption: There is a lengths iterator *some*where in the array.
  // Note that it does no length check
  @tailrec
  private def getLengthsIterator(
    iters: Array[Movable],
    idx: Int = 0): Movable =
    if (iters(idx).isInstanceOf[IndexLengths]) return iters(idx)
    else getLengthsIterator(iters, idx+1)
}
