package julien
package learning
package jforests
package learning
package trees

abstract class Tree {
  val left: Array[Int]
  val right: Array[Int]
  val splits: Array[Int]
  val thresholds: Array[Int]
  val originalThresholds: Array[Double]
  val numLeaves: Int
  var weight: Double

  def backfit(s: Sample): Unit

  def leaf(d: Dataset, idx: Int): Int = {
    if (numLeaves == 1) 0
    else {
      var node = 0
      while (node >= 0) node =
        if (d(idx, splits(node)) <= thresholds(node))
          left(node)
        else
          right(node)
      return ~node
    }
  }

  def leafFromOriginalThreshold(d: Dataset, idx: Int): Int = {
    if (numLeaves == 1) 0
    else {
      var node = 0
      while (node >= 0) node =
        if (d(idx, splits(node)) <= originalThresholds(node))
          left(node)
        else
          right(node)
      return ~node
    }
  }

  def leafFromOriginalThreshold(fVector: Array[Double]): Int = {
    if (numLeaves == 1) 0
    else {
      var node = 0
      while (node >= 0) node =
        if (fVector(splits(node)) <= originalThresholds(node))
          left(node)
        else
          right(node)
      return ~node
    }
  }

  def parent(node: Int): Int = {
    var parent = left.indexOf(node)
    if (parent >= 0) return parent
    parent = right.indexOf(node)
    if (parent >= 0) return parent
    else return -1
  }

  def nodeParents(node: Int): Array[Int] = {
    @tailrec def addParent(n: Int): List[Int] =
      if (n < 0) List.empty
      else n :: addParent(parent(n))

    val parents = addParent(parent(node))
    return parents.reverse.toArray
  }

  def split(leaf: Int, split: TreeSplit): Int = {
    val indexOfNewNonLeaf = numLeaves - 1
    var parent = left.indexOf(~leaf)
    if (parent >= 0) {
      left(parent) = indexOfNewNonLeaf
    } else {
      parent = right.indexOf(~leaf)
      if (parent >= 0) right(parent) = indexOfNewNonLeaf
    }
    splitFeatures(indexOfNewNonLeaf) = split.feature
    thresholds(indexOfNewNonLeaf) = split.threshold
    originalThresholds(indexOfNewNonLeaf) = split.originalThreshold
    left(indexOfNewNonLeaf) = ~leaf
    right(indexOfNewNonLeaf) = ~numLeaves
    numLeaves += 1
    return indexOfNewNonLeaf
  }

  def getNodeLabel(n: Int): Int = if (n < 0) -1 - (~n) else n
}
