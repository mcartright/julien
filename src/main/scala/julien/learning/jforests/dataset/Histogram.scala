package julien.learning
package jforests.dataset

abstract class Histogram(feature: Feature) {
  val numValues = feature.numberOfValues
  val splittable = true

  def -(other: Histogram): Histogram = {
    assume(other.numValues == numValues)

    // Need to perform an actual subtraction here
  }
}
