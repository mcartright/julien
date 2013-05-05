package julien
package eval

object QueryEvaluator {
  def apply(name: String): QueryEvaluator = {
    // TODO: NEED TO IMPLEMENT
    null
  }
}

abstract class QueryEvaluator(val metric: String) {
  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    strictlyEval: Boolean = true): Double

  // TODO
  // Need a natural reducer for this - how to provide?
  def eval[T <: ScoredObject[T]](
    results: QueryResultSet[T],
    judgments: QueryJudgmentSet
  ): Double = {
    var sum = 0.0
    for ((query, result) <- results) {
      assume(judgments.contains(query), s"Judgments missing query $query")
      sum += eval(result, judgments(query))
    }
    sum
  }
}
