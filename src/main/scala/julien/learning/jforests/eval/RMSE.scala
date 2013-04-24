package julien
package learning
package jforests
package eval

class RMSE extends EvaluationMetric(false) {
  def measure(predictions: Array[Double], sample: Sample): Double = {
    val diffs = sample.zip(predictions).map((s, p) => s - p)
    val rmse = sqrt(diffs.map(d => d * d).sum / sample.size)
    return rmse
  }
}
