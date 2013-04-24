package julien.learning
package jforests
package learning.trees.decision

import julien.learning.jforests.learning_

class DecisionTreeSplit extends TreeSplit {

  // Used during update
  case class EntropyInfo(dist: Array[Double],
    entropy: Double = Double.PositiveInfinity,
    threshold: Int = -1)

  def update(dhist: DecisionHistogram) {
    val nonZeroClassCount = dhist.targetDist.filter(_ > 0).sum
    if (nonZeroClassCount == 1) { // indicates a "pure" node (single class)
      gain = 0
      return
    }
    // Figure out where to split
    // Need to calculate the entropy for every
    // possible split, find the min, and use that.
    // Use tail-recursive search for minimum entropy split threshold
    val best = scanForMinEntropy(dhist, 0, 0, dhist.totalCount,
      Array.fill(numClasses)(0.0), dhist.targetDist, EntropyInfo)

    dhist.splittable = best.threshold != -1

    val f: Feature = curTrainSet.dataset.features(feature)
    threshold = f.upperBounds(bestThreshold)
    originalThreshold = f.originalValue(threshold)

    // Adjust my target distributions
    leftTargetDist = best.dist
    rightTargetDist = dhist.targetDist.zip(leftTargetDist).map((a,b) => a - b)

    // Adjust my gain
    gain =
      if (minSplitEntropy.isInfinity) Double.NegativeInfinity
      else entropy(dhist.targetDist) - minSplitEntropy
  }

  @tailrec def scanForMinEntropy(
    dhist: DecisionHistogram,
    t: Int,
    lCount: Int,
    rCount: Int,
    lDist: Array[Double],
    rDist: Array[Double],
    best: EntropyInfo
  ): EntropyInfo {
    // base case - not enough left to check for
    if (rCount < minInstancesPerLeaf || rCount == 0 || t == dhist.numValues - 1)
       best
    else {
      // Check for a new best threshold
      val newbest =
        if (lCount < minInstancesPerLeaf || lCount == 0) best
        else {
          val e = entropy(lDist, rDist)
          if (e >= best.entropy) best else EntropyInfo(lDist, e, t)
        }
      val diff = dhist.perValueCount(t)
      val diffDist = dhist.perValueTarget(t)
      scanForMinEntropy(dhist, t+1, lCount+diff, rCount-diff,
        lDist.zip(diffDist).map((a,b) => a + b),
        rDist.zip(diffDist).map((a,b) => a - b), newbest)
    }
  }
}
