package julien
package eval

import scala.math._

class RootMeanSquredError extends QueryEvaluator {
  def eval[T <: ScoredObject[T]](
    predictions: QueryResult[T],
    actual: QueryJudgments,
    strictlyEval: Boolean
  ): Double = {
    val ps = predictions.map(_.name).toSet
    val diffs = actual.map { case(k,j) =>
      if (ps(j.name)) 1.0 - j.label else 0.0 - j.label
    }
    val rmse = sqrt(diffs.map(d => d * d).sum / actual.size)
    return rmse
  }

  val name: String = "Root Mean Squared Error"
}
