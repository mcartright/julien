package julien.learning
package jforests
package learning.trees.decision

class DecisionTreeLearner(val numClasses: Int, ni: Int, ml: Int)
    extends TreeLearner("DecisionTree") {
  def newTree: Tree = DecisionTree(maxLeaves, numClasses)
  def newSplit: TreeSplit = DecisionTreeSplit(numClasses)
  def newCandidateSplits(nFeat: Int, nInst: Int): CandidateSplitsForLeaf =
    DecisionCandidateSplitsForLeaf(nFeat, numInst, numClasses)
  def newHistogram(f: Feature): Histogram = DecisionHistogram(f, numClasses)
}
