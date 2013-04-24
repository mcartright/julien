package julien
package learning
package jforests
package eval

class Accuracy extends EvaluationMetric(true) {
  def measure(predictions: Array[Double], s: Sample): Double = {
    val numCorrect = predictions.zip(s).filter((p, t) => p == t).size
    numCorrect.toDouble / s.size
  }
}
