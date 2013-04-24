package julien
package learning
package jforests
package learning
package trees

import scala.math._

abstract class CandidateSplitsForLeaf(numFeatures: Int, numInstances: Int) {
  val leafIdx: Int
  val numInstancesInLeaf: Int
  val totalWeightedCount: Double
  val targets: Array[Double]
  val weights: Array[Double]

  val tempIndices = Array.range(0, numFeatures)
  def featureSplit(f: Int) = bestSplitPerFeature(f)
  def bestFeature: Int =
    bestSplitPerFeature.map(_.gain).zipWithIndex.maxBy(_._1)._2

  def bestFeature(splitFraction: Double): Int {
    val maxFeaturesToConsider =
      max((bestSplitPerFeature.length* splitFraction).toInt, 1)
    val candidates =
      Random.shuffle(tempIndices).
        take(maxFeaturesToConsider).
        map(tIdx => (bestSplitPerFeature(tIdx).gain, tIdx)).
        filterNot(_._1.isInfinite)
    if (candidates.isEmpty) -1 else candidates.maxBy(_._1)._2
  }
}
