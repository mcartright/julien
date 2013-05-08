package julien
package retrieval

import scala.annotation.tailrec
import julien.eval.QueryResult
import julien._
import scala.math._

abstract class SimplePreloadingProcessor
    extends SimpleProcessor {
  // For encapsulation - note we assume 1-to-1
  // of features to non-lengths iterators
  case class Sentinel(feat: FeatureOp, iter: Movable, dec: Double)

  // Also want an ordering on the Sentinels based on current location
  object SentinelOrdering extends Ordering[Sentinel] {
    def compare(s1: Sentinel, s2: Sentinel): Int =
      if (s2.iter.isDone) -1
      else if (s1.iter.isDone) 1
      else (s1.iter.at - s2.iter.at)
  }


  /** This is the method that executes after the accumulator has been
    * preloaded, and we still have unfinished iterators. The
    * [[julien.retrieval.MaxscoreProcessor Maxscore]] and
    * [[julien.retrieval.WeakANDProcessor WeakAND]] algorithms both kick in
    * at this point.
    */
  def finishScoring[T <: ScoredObject[T]](
    allSentinels: Array[Sentinel],
    iterators: Array[Movable],
    acc: Accumulator[T]): List[T]

  override def run[T <: ScoredObject[T]](
    acc: Accumulator[T] = DefaultAccumulator[ScoredDocument]()
  ): QueryResult[T] = {
    assume(validated, s"Unable to validate given model/index combination")
    prepare()

    val hackedAcc = acc.asInstanceOf[DefaultAccumulator[ScoredDocument]]
    // Build the sentinel list
    val sentinels = _models.map { feat =>
      val it = feat.movers.filter(_.isSparse).head
      Sentinel(feat, it, feat.upperBound-feat.lowerBound)
    }.toArray

    val iterators = _models.flatMap(_.movers).distinct.toArray
    var drivers = iterators.filter(_.isSparse).toArray
    preLoadAccumulator(sentinels, drivers, iterators, hackedAcc)

    val raw = if (drivers.isEmpty)
      acc.result
    else
      finishScoring(sentinels, iterators, acc)
    return QueryResult(raw)
  }

  // We don't do a finished check here - assume the finished iterators
  // have already been filtered out.
  @tailrec
  final def getMinCandidate(
    drivers: Array[Movable],
    idx: Int = 0,
    m: Int = Int.MaxValue): Int = {
    if (idx == drivers.length) return m
    else getMinCandidate(drivers, idx+1, min(m, drivers(idx).at))
  }

  @tailrec
  final def preLoadAccumulator(
    allSentinels: Array[Sentinel],
    drivers: Array[Movable],
    iterators: Array[Movable],
    acc: Accumulator[ScoredDocument]): Unit = {
    // base case
    if (acc.atCapacity || drivers.isEmpty) {
      return
    } else {
      var i = 0
      // otherwise throw a new candidate in there
      val candidate = getMinCandidate(drivers)
      if (matches(drivers, candidate)) {
        while (i < iterators.length) {
          iterators(i).moveTo(candidate)
          i += 1
        }

        // Another crappy fast loop
        i = 0
        var score = 0.0
        while (i < allSentinels.length) {
          score += allSentinels(i).feat.eval
          i += 1
        }
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
        child.movers.filter(_.isSparse).size == 1 // make sure it's 1-to-1
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
