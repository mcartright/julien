package julien
package eval

import scala.math._

class RMSE extends QueryEvaluator("RMSE") {
  def eval[T <: ScoredObject[T]](
    predictions: QueryResult[T],
    actual: QueryJudgment,
    strictlyEval: Boolean
  ): Double = {
    val ps = predictions.map(_.name).toSet
    val diffs = actual.map { j =>
      if (ps(j.name)) 1.0 - j.label else 0.0 - j.label
    }
    val rmse = sqrt(diffs.map(d => d * d).sum / actual.size)
    return rmse
  }
}
