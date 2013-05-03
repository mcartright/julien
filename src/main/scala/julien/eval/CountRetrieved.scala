package julien
package eval

class CountRetrieved(n: String) extends QueryEvaluator(n) {
  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    strictlyEval: Boolean): Double = result.size.toDouble
}
