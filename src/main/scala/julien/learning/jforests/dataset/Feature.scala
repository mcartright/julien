package julien.learning
package jforests.dataset

case class Feature(
  val bins: NumericArray,
  val upperBounds: Array[Int],
  val name: String,
  val min: Double,
  val max: Double,
  val factor: Double,
  val useLogScale: Boolean) {

  def originalValue(scaled: Int): Double = {
    val v = scaled / factor
    if (useLogScale)
      scala.math.exp(v) + min - 1
    else
      v + min
  }

  def numberOfValues: Int = upperBounds.length

  def subSampleFeature(indices: Array[Int]): Feature =
    Feature(bins.subSample(indices),
      upperBounds, name, min, max, factor, useLogScale)
}
