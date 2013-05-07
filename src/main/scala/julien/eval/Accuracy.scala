package julien
package eval

class Accuracy extends QueryEvaluator("Accuracy") {
  def eval[T <: ScoredObject[T]](
    predictions: QueryResult[T],
    actual: QueryJudgment,
    strictlyEval: Boolean
  ): Double = {
    val ps = predictions.map(_.name).toSet
    val numCorrect = actual.map { judgment =>
      if (judgment.label == 1) {
        // It's actually relevant
        if (ps(judgment.name)) 1 // TP
        else 0 // FN
      } else {
        if (ps(judgment.name)) 0 // FP
        else 1 // TN
      }
    }.sum
    numCorrect.toDouble / actual.size
  }
}
