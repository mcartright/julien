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
  */
class MaxscoreProcessor extends SimpleProcessor {
  // For encapsulation - note we assume 1-to-1
  // of features to non-lengths iterators
  case class Sentinel(feat: FeatureOp, iter: GIterator, dec: Double)

  override def validated: Boolean = {
    val simpleCheck = super.validated
    if (simpleCheck == false) return simpleCheck

    // Structural check for something like:
    // Combine(f1, f2, f3, ...)
    // All top-level operators in _models should look like this.
    var result: Boolean = _models.forall(_.isInstanceOf[Combine])
    // Make sure for each combiner, each child is a feature with
    // actual bounds.
    for (combiner <- _models) {
      // Do children explicitly so we don't traverse the entire
      // subtree rooted here.
      result = result && combiner.children.forall { child =>
        child.isInstanceOf[FeatureOp] &&
        isBounded(child) &&
        child.iHooks.filter(_.isSparse).size == 1 // make sure it's 1-to-1
      }
    }
    return result
  }

  override def prepare(): Unit = {
    super.prepare() // Do all the stuff we normally do

    // Verified earlier that the models here consist of a
    // bunch of combines with bounded FeatureOp children,
    // (i.e. a bag-of-words type structure).
    _models = _models.flatMap(c => c.children.map(_.asInstanceOf[FeatureOp]))
  }

  override def run: List[ScoredDocument] = {
    // Need this in scope already
    var threshold = new Score(0.0)


    // Helper functions that are internally defined
    @tailrec
    def conditionalAddSentinel(
      sents: Seq[Sentinel],
      idx: Int,
      oldScore: Score): Tuple2[Score, Int] =
      // base case
      if (idx == sents.size || oldScore < threshold)
        (oldScore, idx)
      else
        conditionalAddSentinel(sents, idx+1, oldScore + sents(idx).feat.eval)

    @tailrec
    def getSentinelIndex(
      sents: Seq[Sentinel],
      idx: Int,
      currentScore: Score): Int =
      if (idx == sents.size || currentScore < threshold)
        return idx
      else
        getSentinelIndex(sents, idx+1, currentScore - sents(idx).dec)


    // Main Method
    assume(validated, s"Unable to validate given model/index combination")
    prepare()

    // Build the sentinel list - sort is on idf
    val sentinels = _models.map { feat =>
      val it = feat.iHooks.filter(_.isSparse).head.underlying
      Sentinel(feat, it, feat.upperBound-feat.lowerBound)
    } sortBy { s =>
      s.iter.totalEntries
    }

    // establish upper bound of all features - every document starts at
    // this value and is progressively lowered to the true score
    val startingScore = sentinels.map(_.feat.upperBound).sum
    val resultQueue = PriorityQueue[ScoredDocument]()
    // direct access to the underlying iterators
    val iterators = _models.flatMap(m => m.iHooks.map(_.underlying)).toSet
    val numResults:Int = 100
    // Have to warm up the queue
    for (i <- 0 until numResults) {
      val candidate =
        sentinels.map(_.iter).filterNot(_.isDone).map(_.currentCandidate).min
      iterators.foreach(_.syncTo(candidate))
      val score = sentinels.map(_.feat.eval).sum
      resultQueue.enqueue(ScoredDocument(candidate, score))
    }

    // At this point we've scored "numResults" docs, and we need to find the
    // next candidate to score, however we are changing strategy here.

    // Now start looking for cutoffs
    var sidx = getSentinelIndex(sentinels, 0, startingScore)

    // HACK - would like this to be cleaner
    val lengths = iterators.filter(_.isInstanceOf[LI]).head

    // Scala does not natively support a "break" construct, so let's avoid it.
    // We simply need to set the candidate once before entering the while
    // loop, then check at the end of the loop
    var candidate =
      sentinels.take(sidx).
        map(_.iter).
        filterNot(_.isDone).
        map(_.currentCandidate).min

    while (candidate < Int.MaxValue) {
      val drivers = sentinels.take(sidx).map(_.iter)
      if (drivers.exists(_.hasMatch(candidate))) {
        threshold = resultQueue.head.score
        (lengths +: drivers).foreach(_.syncTo(candidate))
        // Sum the active sentinels
        val senscore = sentinels.take(sidx).map(_.feat.eval).sum

        // Now add the rest of them until we're done or it's below threshold
        // doing this via tail recursion
        val (score, pos) = conditionalAddSentinel(sentinels, sidx, senscore)

        if (pos == sentinels.size && score > threshold) {
          resultQueue.dequeue // We know this will happen
          resultQueue.enqueue(ScoredDocument(candidate, score))
          sidx = getSentinelIndex(sentinels, 0, startingScore)
        }
      }

      // Grab next candidate
      candidate =
        sentinels.take(sidx).
          map(_.iter).
          filterNot(_.isDone).
          map(_.currentCandidate).min
    }

    // All done - return
    resultQueue.reverse
  }
}
