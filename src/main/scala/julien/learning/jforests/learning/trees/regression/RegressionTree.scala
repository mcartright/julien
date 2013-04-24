package julien.learning
package jforests.learning.trees.regression

class RegressionTree extends Tree {
  def copy: RegressionTree = throw new UnsupportedOperationException
  def backfit(sample: Sample) {
    val sumPerLeaf = Array.fill(numLeaves)(0.0)
    val weightedCountPerLeaf = Array.fill(numLeaves)(0.0)
    for (i <- 0 until sample.size; iid = sample.indicesInDataset(i)) {
      val l = leaf(sample.dataset, iid(i))
      sumPerLeaf(l) += sample.targets(i) * sample.weights(i)
      weightedCountPerLeaf(l) += sample.weights(i)
    }

    for (l <- 0 until numLeaves; if weightedCountPerLeaf(l) > 0) {
      val newOutput = sumPerLeaf(l) / weightedCountPerLeaf(l)
      setLeafOutput(l, newOutput) // TODO: make appropriate update method
      var p = parent(~l)
      while (p >= 0) {
        sumPerInternalNode(p) += newOutput
        countPerInternalNode(p) += weightedCountPerLeaf(l)
        p = parent(p)
      }
    }

    val hasZeroCountLeaf = weightedCountPerLeaf.exists(_ == 0)
    if (hasZeroCountLeaf) {
      for (l <- 0 until numLeaves; if weightedCountPerLeaf(l) == 0) {
        val p = parent(~l)
        while (p >= 0) {
          if (countPerInternalNode(p) > 0) {
            // setLeafOutput
            // TODO: as break. Refactor.
          }
          p = parent(p)
        }
      }
    }
  }

  def split(leaf: Int, split: RegressionTreeSplit): Int = {
    val newParentIndex = super.split(leaf, split)
    leafOutputs(leaf) = split.leftOutput
    leafOutputs(numLeaves-1) = split.rightOutput
    return newParentIndex
  }

  @inline def outputs(d: Dataset): Array[Double] =
    Array.range(0, d.numInstances).map(i => getOutput(d, i))

  @inline def output(d: Dataset, i: Int): Double = leafOutputs(leaf(d, i))

  def incrementLeafOutputs(constant: Double) {
    if (constant == 0) return
      leafOutputs.zipWithIndex.foreach((o,i) => setLeafOutput(i, o + constant))
  }

  def multiplyLeafOutputs(factor: Double) {
    if (factor == 1.0) return
      leafOutputs.zipWithIndex.foreach((o,i) => setLeafOutput(i, o * factor))
  }

  def setLeafOutput(leaf: Int, output: Double) {
    leafOutputs(leaf) = if (maxLeafOutput <= 0) output
    else if (output > 0) min(output, maxLeafOutput)
    else max (output, -maxLeafOutput)
  }
}
