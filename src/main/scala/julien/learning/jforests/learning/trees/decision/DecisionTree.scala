package julien.learning
package jforests
package learning.trees.decision

class DecisionTree extends Tree {

  def backfit(sample: Sample) {
    val distPerLeaf = Array.ofDim[Double](numLeaves)(numClasses)
    for (i <- 0 until sample.size; iid = sample.indicesInDataset) {
      val l = leaf(sample.dataset, iid(i))
      distPerLeaf(l)(sample.targets(i).toInt) += sample.weights(i)
    }
    val weightedCountPerLeaf = Array.ofDim[Double](numLeaves)
    for (l <- 0 until numLeaves; c <- 0 until numClasses)
      weightedCountPerLeaf(l) += distPerLeaf(l)(c)

    val distPerInternalNode = Array.ofDim(numLeaves-1)(numClasses)
    for (l <- 0 until numLeaves; if weightedCountPerLeaf > 0) {
      // setLeafTargetDistribution - why?
      var p = parent(~l)
      while (p >= 0) {
        for (c <- 0 until numClasses)
          distPerInternalNode(p)(c) += distPerLeaf(l)(c)
        p = parent(p)
      }
    }
    var hasZeroCountLeaf = weightedCountPerLeaf.exists(_ == 0)
    if (hasZeroCountLeaf) {
      val weightedCountPerInternalNode = distPerInternalNode.map(_.sum)
      for (l <- 0 until numLeaves; if weightedCountPerLeaf(l) == 0) {
        var p = parent(~l)
        while (p >= 0) {
          if (weightedCountPerInternalNode(p) > 0) {
            // setLeafTargetDistribution
            // TODO: was break - refactor to tail recursion
          }
          p = parent(p)
        }
      }
    }
  }

  def normalizeLeafTargetDistributions(leaf: Int) {
    val sum = leafTargetDistributions(leaf).sum
    leafTargetDistributions(leaf) =
      leafTargetDistributions(leaf).map(_ / sum)
  }

  // TODO: Implicit casting downward might not be a bad
  // idea
  implicit def split2dsplit(s: TreeSplit): DecisionTreeSplit =
    s.asInstanceOf[DecisionTreeSplit]

  override split(leaf: Int, split: DecisionTreeSplit): Int = {
    val newParentIdx = super.split(leaf, split)
    for (c <- 0 until numClasses) {
      leafTargetDistributions(leaf)(c) = split.leftTargetDist(c)
      leafTargetDistributions(numLeaves-1)(c) =
        split.rightTargetDist(c)
    }
    normalizeLeafTargetDistributions(leaf)
    normalizeLeafTargetDistributions(numLeaves-1)
    return newParentIndex
  }

  def getPredictions(d: Dataset): Array[Int] =
    Array.range(0, d.numInstances).map(i => classify(d, i))

  def classify(d: Dataset, instanceIdx: Int): Int {
    leafTargetDistributions(leaf(d, instanceIdx)).
      zipWithIndex.
      maxBy(_._1)._2
  }

  def copy: DecisionTree = throw new UnsupportedOperationException
}
