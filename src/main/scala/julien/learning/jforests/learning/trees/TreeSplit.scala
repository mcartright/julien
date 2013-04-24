package julien.learning
package jforests
package learning
package trees

abstract class TreeSplit(
  feature: Int,
  threshold: Int,
  originalThreshold: Double,
  gain: Double) {
  def update(split: CandidateSplitsForLeaf)
}
