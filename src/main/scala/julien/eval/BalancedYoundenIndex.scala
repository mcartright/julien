package julien
package eval

class BalancedYoundenIndex extends QueryEvaluator("BalancedYoundenIndex") {
  val threshold = 0.5

  def eval[T <: ScoredObject[T]](
    predictions: QueryResult[T],
    actual: QueryJudgment,
    strictlyEval: Boolean
  ): Double = {
    val ps = predictions.map(_.name).toSet
    val paired = actual.map { j => (j.label, (if (ps(j.name)) 1.0 else 0.0)) }

    // a pair is (target, prediction) - bin and count
    val (predictTrue, predictFalse) = paired.partition(_._2 > threshold)
    val (tp, fp) = predictTrue.partition(_._1 > threshold)
    val (tn, fn) = predictFalse.partition(_._1 < threshold)
    val sensitivity = tp.size.toDouble / (tp.size + fn.size)
    val specificity = tn.size.toDouble / (tn.size + fp.size)
    return scala.math.min(sensitivity, specificity)
  }
}
