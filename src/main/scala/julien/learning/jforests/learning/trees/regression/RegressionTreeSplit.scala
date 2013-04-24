package julien.learning
package jforests.learning.trees.regression

class RegressionTreeSplit extends TreeSplit {
  def update(rhist: RegressionHistogram) {
    val ub = rhist.numValues - 1
    val limit = if (!randomizedSplits) ub
    else {
      // Find a random stopping place to look for the split
      val counts = tailsum(rhist.perValueCount)
      var lastT =
        counts.lastIndexWhere(c => rhist.totalCount - c < minInstancePerLeaf)
      val maxIdx = if (lastT == -1) ub else lastT + 1
      val lastT =
        counts.lastIndexWhere(_ < minInstancesPerLeaf, min(maxIdx, ub))
      val minIdx = if (lastT == -1) 0 else lastT
      minIdx + Random.nextInt(maxIdx - minIdx)  // limit gets this
    }

    // Tail-recursive search for the best-gain split
    val best = bestGainScan(rhist, limit, 0,
      0, rhist.totalCount,
      0.0, rhist.sumTargets,
      0.0, rhist.totalWeightedCount, GainInfo())

    rhist.splittable = !best.gain.isInfinity
    val f = curTrainSet.dataset.features(feature)
    threshold = feature.upperBounds(best.threshold)
    originalThreshold = feature.originalValue(threshold)
    leftOutput = best.targetSum / best.weightedCount
    rightOutput = (rhist.sumTargets - best.targetSum) /
      (rhist.totalWeightedCount - best.weightedCount)
    gain = best.gain -
      (rhist.sumTargets * rhist.sumTargets) / rhist.totalWeightedCount
  }

  // Stole this diddy from the APScorer higher up in the learning lib.
  // Need to consolidate when I'm not feeling lazy.
  // Also note that the one in APScorer adds up hits - this one literally
  // does "sum so far"
  @tailrec
  def tailsum(hits: List[Int], sum: Int = 0): List[Int] = hits match {
    case Nil => List.empty
    case head :: tail => sum+head :: tailsum(tail, sum+head)
  }

  @tailrec
  def bestGainScan(
    rhist: RegressionHistogram,
    limit: Int,
    t: Int,
    lCount: Int, rCount: Int,
    sumLeft: Double, sumRight: Double,
    wlCount: Double, wrCount: Double,
    best: GainInfo
  ): GainInfo = {
    if (rCount < minInstancesPerLeaf || rCount == 0 || t >= limit) best
    else {
      val newbest = if (lCount < minInstancesPerLeaf || lCount == 0) best
      else {
        val gain =
          (sumLeft * sumLeft) / wlCount + (sumRight * sumRight) / wrCount
        if (gain <= best.gain) best else GainInfo(gain, t, sumLeft, wlCount)
      }
      val countDelta = rhist.perValueCount(t)
      val weightedDelta = rhist.perValueWeightedCount(t)
      val sumDelta = rhist.perValueSumTargets(t)
      bestGainScan(rhist, limit, t+1,
        lCount + countDelta, rCount - countDelta,
        sumLeft + sumDelta, sumRight - sumDelta,
        wlCount + weightedDelta, wrCount - weightedDelta,
        newbest)
    }
  }
  case class GainInfo(
    gain: Double = Double.NegativeInfinity, threshold: Int = 0,
    targetSum = Double.NaN, weightedCount: Double = -1)
}
