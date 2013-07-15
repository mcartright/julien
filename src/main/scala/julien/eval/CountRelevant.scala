package julien
package eval

/** Counts the number of documents judged to be relevant */
class CountRelevant() extends QueryEvaluator() {
  def eval[T <: ScoredObject](
    result: QueryResult[T],
    judgment: QueryJudgments,
    strictlyEval: Boolean): Double = numRelevant(judgment)

  val name: String = "Relevant"
}
