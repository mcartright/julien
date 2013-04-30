package julien
package retrieval

import scala.annotation.tailrec
import julien._
import scala.math._

abstract class SimplePreloadingProcessor
    extends SimpleProcessor {
  /** This is the method that executes after the accumulator has been
    * preloaded, and we still have unfinished iterators. The
    * [[julien.retrieval.MaxscoreProcessor Maxscore]] and
    * [[julien.retrieval.WeakANDProcessor WeakAND]] algorithms both kick in
    * at this point.
    */
    def finishScoring[T <: ScoredObject[T]](
      allSentinels: Seq[Sentinel],
      iterators: Set[GIterator],
      acc: Accumulator[T]): List[T]

  override def run[T <: ScoredObject[T]](acc: Accumulator[T] =
    DefaultAccumulator[ScoredDocument]()): List[T] = {
    assume(validated, s"Unable to validate given model/index combination")
    prepare()

    val hackedAcc = acc.asInstanceOf[DefaultAccumulator[ScoredDocument]]
    // Build the sentinel list
    val sentinels = _models.map { feat =>
      val it = feat.iHooks.filter(_.isSparse).head.underlying
      Sentinel(feat, it, feat.upperBound-feat.lowerBound)
    }

    val iterators = _models.flatMap(_.iHooks).map(_.underlying).toSet
    var drivers = iterators.filterNot(_.hasAllCandidates)
    preLoadAccumulator(sentinels, drivers, iterators, hackedAcc)

    if (drivers.isEmpty) return acc.result
    else finishScoring(sentinels, iterators, acc)
  }

  // For encapsulation - note we assume 1-to-1
  // of features to non-lengths iterators
  case class Sentinel(feat: FeatureOp, iter: GIterator, dec: Double)

  // Also want an ordering on the Sentinels based on current location
  object SentinelOrdering extends Ordering[Sentinel] {
    def compare(s1: Sentinel, s2: Sentinel): Int =
      if (s2.iter.isDone) -1
      else if (s1.iter.isDone) 1
      else (s1.iter.currentCandidate - s2.iter.currentCandidate)
  }

  // We don't do a finished check here - assume the finished iterators
  // have already been filtered out.
  @tailrec
  final def getMinCandidate(
    drivers: Set[GIterator],
    m: Int = Int.MaxValue): Int = {
    if (drivers.isEmpty) return m
    else getMinCandidate(drivers.tail, min(m, drivers.head.currentCandidate))
  }

  @tailrec
  final def preLoadAccumulator(
    allSentinels: Seq[Sentinel],
    drivers: Set[GIterator],
    iterators: Set[GIterator],
    acc: Accumulator[ScoredDocument]): Unit = {
    // base case
    if (acc.atCapacity || drivers.isEmpty) {
      return
    } else {
      // otherwise throw a new candidate in there
      val candidate = getMinCandidate(drivers)
      if (drivers.exists(_.hasMatch(candidate))) {
        iterators.foreach(_.syncTo(candidate))
        val score = allSentinels.map(_.feat.eval).sum
        debug(s"preload: ($candidate, $score)")
        acc += ScoredDocument(candidate, score)
      }
      drivers.foreach(_.movePast(candidate))
      // Remove finished iterators from the remaining list at the next
      // call - saves checking later
      preLoadAccumulator(
        allSentinels,
        drivers.filterNot(_.isDone),
        iterators,
        acc)
    }
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
}
