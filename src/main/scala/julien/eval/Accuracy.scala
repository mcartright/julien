package julien
package eval

/** Reports the accuracy for a given query result/judgment pair.
  * Accuracy is defined as:
  *
  * Acc = (true pos + true neg) / (all predictions)
  *
  */
class Accuracy extends QueryEvaluator() {
  def eval[T <: ScoredObject](
    predictions: QueryResult[T],
    actual: QueryJudgments,
    strictlyEval: Boolean
  ): Double = {
    val ps = predictions.map(_.name).toSet
    val counts = for (judgment <- actual) yield {
      if (judgment.isRelevant) {
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
