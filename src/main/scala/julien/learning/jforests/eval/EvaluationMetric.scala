package julien
package learning
package jforests
package eval

abstract class EvaluationMetric(largerIsBetter: Boolean) {
  def measure(predictions: Array[Double], sample: Sample): Double
  def isFirstBeter(first: Double, second: Double, tolerance: Double): Boolean =
    if (second.isNaN) true
    else if (isLargerBetter) (first * (1 + tolerance)) > second
    else (first * (1 - tolerance)) < second
}
