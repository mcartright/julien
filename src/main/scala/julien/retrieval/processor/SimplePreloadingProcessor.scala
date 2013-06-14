package julien
package retrieval
package processor

import scala.annotation.tailrec
import julien.eval.QueryResult
import julien._
import julien.behavior._
import scala.math._

abstract class SimplePreloadingProcessor private[processor] (root: Feature)
    extends SimpleProcessor(root: Feature) {
  import QueryProcessor.isDone

  // For encapsulation - note we assume 1-to-1
  // of features to non-lengths iterators
  case class Sentinel(feat: Feature, iter: Movable, dec: Double)

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
    acc: Accumulator[T]
  ): QueryResult[T] = {

    val hackedAcc = acc.asInstanceOf[DefaultAccumulator[ScoredDocument]]

    // Build the sentinel list - we know we can just go under the root
    val models = root.children.map(_.asInstanceOf[Feature])
    val sentinels = models.map { feat =>
      val it = feat.movers.filter(_.isSparse).head
      Sentinel(feat, it, feat.upperBound - feat.lowerBound)
    }.toArray

    val iterators = models.flatMap(_.movers).distinct.toArray
    for (it <- iterators) it.reset
    var drivers = iterators.filter(_.isSparse).toArray
    preLoadAccumulator(sentinels, drivers, iterators, hackedAcc)

    val raw = if (isDone(drivers))
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

  final def preLoadAccumulator(
    allSentinels: Array[Sentinel],
    drivers: Array[Movable],
    iterators: Array[Movable],
    acc: Accumulator[ScoredDocument]): Unit = {

    var i = 0
    var candidate = Int.MaxValue
    while (i < drivers.length) {
      val drv = drivers(i)
      if (!drv.isDone && drv.at < candidate) candidate = drv.at
      i += 1
    }

    while (!acc.atCapacity && !isDone(drivers)) {
      // Score our candidate
      i = 0
      var score = 0.0
      while (i < allSentinels.length) {
        score += allSentinels(i).feat.eval(candidate)
        i += 1
      }

      acc += ScoredDocument(candidate, score)

      // And select the next one
      i = 0
      var newCandidate = Int.MaxValue
      while (i < drivers.length) {
        val drv = drivers(i)
        if (!drv.isDone) {
          val tmp = drv.movePast(candidate)
          if (tmp < newCandidate) newCandidate = tmp
        }
        i += 1
      }
      candidate = newCandidate
    }
  }
}
