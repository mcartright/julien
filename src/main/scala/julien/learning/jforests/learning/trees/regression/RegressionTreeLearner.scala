package julien.learning
package jforests.learning.trees.regression

class RegressionTreeLearner extends TreeLearner("RegressionTree") {
  def newTree: Tree = new RegressionTree
  def newSplit: TreeSplit = new RegressionTreeSplit
  def newCandidateSplits(nFeat: Int, nInst: Int): CandidateSplitsForLeaf =
    RegressionCandidateSplitsForLeaf(nFeat, numInst)
  def newHistogram(f: Feature): Histogram = RegressionHistogram(f)
}
