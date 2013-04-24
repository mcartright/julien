package julien.learning
package jforests
package learning.trees

abstract class TreeLearner(n: String) extends LearningModule(n) {
  val ROOT_LEAF_INDEX = 0
  def newTree: Tree
  def newSplit: TreeSplit
  def newHistogram(f: Feature): Histogram

  override def learn(trainSet: Sample, validSet: Sample): Ensemble = {
    val minInstancesPerLeaf = (trainSet.size * minInstancePctPerLeaf) / 100.0
    val selectedFeatures = featuresToDiscard.map(!_)
    perNodeHistograms.flatMap.foreach(_.splittable = true)
    val tree = newTree()
    val candidateSplitsForSmallerChild = CandidateSplitsForLeaf(
      ROOT_LEAF_INDEX,
      trainTreeLeafInstances,
      trainSet
    )

    var parentNodeIdx = -1
    var smallerChildIdx = ROOT_LEAF_INDEX
    val candidateSplitsForLargerChild = CandidateSplitsForLeaf()
    updateSplits()
    val idx = candidateSplitsForSmallerChild.leafIndex
    perLeafBestSplit(idx) = bestSplit(candidateSplitsForSmallerChild)
    val rootSplit = perLeafBestSplit(ROOT_LEAF_INDEX)
    if (rootSplit.isInfinity) return None

    val newInteriorNodeIndex = tree.split(ROOT_LEAF_INDEX, rootSplit)
    val right = ~tree.right(newInteriorNodeIndex)
    val left, bestLeaf, parentNodeIndex = ROOT_LEAF_INDEX
    trainTreeLeafInstances.split(
      bestLeaf,
      trainSet.dataset,
      rootSplit.feature,
      rootSplit.threshold,
      right, trainSet.indicesInDataset
    )

    for (k <- 2 until maxLeaves) {
      val leftSize = trainTreeLeafInstances.numberOfInstancesInLeaf(left)
      val rightSize = trainTreeLeafInstances.numberOfInstancesInLeaf(right)
      if (isTooBig(rightSize) || isTooBig(leftSize)) {
        if (leftSize < rightSize) {
          val dist = perNodeHistograms(right)
          perNodeHistograms(right) = perNodeHistograms(left)
          perNodeHistograms(left) =
            if (dist != null) dist
            else newHistogramArray()
          largerChildIndex = right
          smallerChildIndex = left
        } else {
          if (perNodeHistograms(right) == null)
            perNodeHistograms(right) = newHistogramArray()
          largerChildIndex = left
          smallerChildIndex = right
        }
        updateSplits()
        var idx = candidateSplitsForSmallerChild.leafIndex
        perLeafBestSplit(idx) = bestSplit(candidateSplitsForSmallerChild)
        idx = candidateSplitsForLargerChild.leafIndex
        perLeafBestSplit(idx) = bestSplit(candidateSplitsForLargerChild)
      } else {
        perLeafBestSplit(left).gain = Double.NegativeInfinity
        perLeftBestSplit(right) = newSplit(gain = Double.NegativeInfinity)
      }

      val bestLeaf = perLeafBestSplit.zipWithIndex.maxBy(_._1.gain)._2
      val bestLeafSplit = perLeafBestSplit(bestLeaf)
      if (bestLeafSplit.gain.isNaN || bestLeafSplit.gain <= 0) {
        // TODO: was break. FIX.
      }

      val newNodeIndex = tree.split(bestLeaf, bestLeafSplit)
      left = bestLeaf
      right = ~tree.right(newNodeIndex)
      parentNodeIndex = bestLeaf
      trainTreeLeafInstances.split(
        bestLeaf,
        trainSet.dataset,
        bestLeafSplit.feature,
        bestLeafSplit.threshold,
        right, trainSet.indicesInDataset
      )
    }

    if (parentLearner.isDefined)
      parentLearner.get.postProcess(tree, trainTreeLeafInstances)
    Ensemble(tree)
    }

  def updateSplits() {
    for ((status, f) <- selectedFeatures.zipWithIndex; if status) {
      if (parentNodeIndex != -1 &&
        !perNodeHistograms(parentNodeIndex)(f).splittable)
        perNodeHistograms(smallerChildIndex)(f).splittable = false
      else {
        candidateSplitsForSmallerChild.
          featureSplit(f).
          update(perNodeHistograms(smallerChildIndex)(f))
        if (parentNodexIndex != -1) {
          perNodeHistograms(largerChildIndex)(f).
            subtractFromMe(perNodeHistograms(smallerChildIndex)(f))
          candidateSplitsForLargerChild.featureSplit(f).
            update(perNodeHistograms(largerChildIndex)(f))
        }
      }
    }
  }

  def bestSplit(csfl: CandidateSplitsForLeaf) : TreeSplit {
    val bestFeature = if (featureSamplingPerSplit < 1.0)
      csfl.bestFeature(featureSamplingPerSplit)
    else
      csfl.bestFeature

    if (bestFeature < 0)
      csfl.getFeatureSplit(0).copy(gain = Double.NegativeInfinity)
    else
      csfl.getFeatureSplit(bestFeature)
  }

  @inline def isTooBig(numLeaves: Int) = numLeaves >= 2 * minInstancesPerLeaf
}
