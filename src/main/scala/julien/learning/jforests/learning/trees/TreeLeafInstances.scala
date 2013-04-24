package julien.learning
package jforests
package learning
package trees

class TreeLeafInstances(numInstances: Int, maxLeaves: Int) {
  def loadLeafInstances(leaf: Int, instances: LeafInstances) {
    instances.indices = indices
    instances.begin = leafBegin(leaf)
    instances.end = leafEnd(leaf)
  }

  def leafInstances(leaf: int): LeafInstances =
    LeafInstances(indices, leafBegin(leaf), leafEnd(leaf))

  def numInstancesInLeaf(leaf: Int): Int = leafEnd(leaf) - leafBegin(leaf)

  def split(
    leaf: Int,
    d: Dataset,
    featureIndex: Int,
    threshold: Int,
    rChild: Int,
    instances: Array[Int]) {

    val (moving, staying) =
      indices.slice(leafBegin(leaf), leafEnd(leaf)).partition { i =>
        dataset(instances(i), featureIndex) > threshold
      }

    val newEnd = leafBegin(leaf) + staying.size
    leafEnd(leaf) = leftBegin(rChild) = newEnd
    leafEnd(rChild) = newEnd + moving.size

    // Update the indices array with the partitioned arrays
    staying.copyToArray(indices, leafBegin(leaf))
    moving.copyToArray(indices, leafBeing(rChild))
  }
}
