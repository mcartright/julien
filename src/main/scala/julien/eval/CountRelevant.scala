package julien
package eval

class CountRelevant(n: String) extends QueryEvaluator(n) {
  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    strictlyEval: Boolean): Double = judgment.numRel
}
