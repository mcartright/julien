package julien.learning
package jforests.dataset

class Dataset(val features: Array[Feature], val targets: Array[Double]) {
  def numInstances = targets.length
  def numFeatures = features.length

  def featureValue(instanceIdx: Int, featureIdx: Int): Int = {
    val f = features(featureIdx)
    return f.upperBounds(f.bins(instanceIdx))
  }

  def originalFeatureValue(instanceIdx: Int, featureIdx: Int): Int = {
    val f = features(featureIdx)
    val scaledValue = f.upperBounds(f.bins(instanceIdx))
    return f.originalValue(scaledValue)
  }

  def maxFeatureValues: Int = features.map(_.numberOfValues).max
  def featureIdx(featName: String): Int = features.map(_.name).indexOf(featName)
}
