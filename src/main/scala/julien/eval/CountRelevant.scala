package julien
package eval

class CountRelevant() extends QueryEvaluator() {
  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgments,
    strictlyEval: Boolean): Double = numRelevant(judgment)

  val name: String = "Relevant"
}
