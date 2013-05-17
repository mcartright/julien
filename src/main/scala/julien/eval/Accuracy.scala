package julien
package eval

class Accuracy extends QueryEvaluator() {
  def eval[T <: ScoredObject[T]](
    predictions: QueryResult[T],
    actual: QueryJudgments,
    strictlyEval: Boolean
  ): Double = {
    val ps = predictions.map(_.name).toSet
    val counts = for (judgment <- actual) yield {
      if (judgment.label == 1) {
        // It's actually relevant
        if (ps(judgment.name)) 1 // TP
        else 0 // FN
      } else {
        if (ps(judgment.name)) 0 // FP
        else 1 // TN
      }
    }
    val numCorrect = counts.sum
    numCorrect.toDouble / actual.size
  }

  val name: String = "Accuracy"
}
