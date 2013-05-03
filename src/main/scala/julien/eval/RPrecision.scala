package julien
package eval

class RPrecision(n: String) extends QueryEvaluator(n) {
    def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    strictlyEval: Boolean): Double = {
      val relCount = judgment.numRel
      val retCount = result.size
      // Our return value
      if (relCount > retCount) 0.0
      else new Precision(relCount).eval(result, judgment)
    }
}
