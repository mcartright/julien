package julien
package eval

class RPrecision() extends QueryEvaluator() {

  def eval[T <: ScoredObject](
    result: QueryResult[T],
    judgment: QueryJudgments,
    strictlyEval: Boolean): Double = {
    val relCount = numRelevant(judgment)
    val retCount = result.size
    // Our return value
    if (relCount > retCount) 0.0
      else new Precision(relCount).eval(result, judgment)
  }

  val name: String = "R Precision"
}
