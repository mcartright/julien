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
  */
class WeakANDProcessor(factor: Double = 1.0) extends SimpleProcessor {
  // For encapsulation - note we assume 1-to-1
  // of features to non-lengths iterators
  case class Sentinel(feat: FeatureOp, iter: GIterator)

  // Also want an ordering on the Sentinels
  object SentinelOrdering extends Ordering[Sentinel] {
    def compare(s1: Sentinel, s2: Sentinel): Int =
      if (s2.iter.isDone) -1
      else if (s1.iter.isDone) 1
      else (s1.iter.currentCandidate - s2.iter.currentCandidate)
  }

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

  override def run[T <: ScoredObject[T]](acc: Accumulator[T] =
    DefaultAccumulator[ScoredDocument]()): List[T] = {
    // Main Method
    assume(validated, s"Unable to validate given model/index combination")
    prepare()

    val hackedAcc = acc.asInstanceOf[DefaultAccumulator[ScoredDocument]]
    // Build the sentinel list - sort will change over time
    val sentinels = _models.map { feat =>
      val it = feat.iHooks.filter(_.isSparse).head.underlying
      Sentinel(feat, it)
    }.toArray

    // direct access to the underlying iterators
    val iterators = _models.flatMap(_.iHooks.map(_.underlying)).toSet
    // Have to warm up the queue
    for (i <- 0 until hackedAcc.limit) {
      val candidate =
        sentinels.map(_.iter).filterNot(_.isDone).map(_.currentCandidate).min
      iterators.foreach(_.syncTo(candidate))
      val score = sentinels.map(_.feat.eval).sum
      hackedAcc += ScoredDocument(candidate, score)
    }

    // At this point we've scored "numResults" docs, and we need to find the
    // next candidate to score, however we are changing strategy here.

    // This is the WeakAND strategy - before scoring fully, we decide whether
    // this document has the potential to make it into the final list. If so,
    // it's fully scored.

    // Start with a full sort - the only one needed
    var sortedSentinels = sentinels.sorted(SentinelOrdering)
    val scoreMinimum = sortedSentinels.map(_.feat.lowerBound).sum

    var running = true
    var threshold = hackedAcc.head.score * factor // Soft limit
    var lastScored = -1
    while (running) {
      val pivotPos = findPivot(sortedSentinels, scoreMinimum, threshold)
      if (pivotPos == -1 || sortedSentinels(pivotPos).iter.isDone) {
        // invariants not maintained - time to stop
        running = false
      } else {
        val pivot = sortedSentinels(pivotPos).iter.currentCandidate
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
          if (score > threshold) {
            // Pass muster, put it in the queue and update the threshold
            hackedAcc += ScoredDocument(pivot, score)
            threshold = hackedAcc.head.score * factor
          }
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
