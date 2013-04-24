package julien
package learning
package jforests
package eval



class BalancedYoundenIndex extends EvaluationMetric(true) {
  val threshold = 0.5

  def measure(predictions: Array[Double], sample: Sample): Double = {
    val paired = s.zip(predictions)
    // a pair is (target, prediction) - bin and count
    val (predictTrue, predictFalse) = paired.partition(_._2 > threshold)
    val (tp, fp) = predictTrue.partition(_._1 > threshold).map(_.size)
    val (tn, fn) = predictFalse.partition(_._1 < threshold).map(_.size)
    val sensitivity = tp.toDouble / (tp + fn)
    val specificity = tn.toDouble / (tn + fp)
    return scala.math.min(sensitivity, specificity)
}
