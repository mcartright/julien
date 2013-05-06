package julien
package eval

class AvgPrecision(n: String) extends QueryEvaluator(n) {
  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    strictlyEval: Boolean): Double = {
    if (judgment.numRel == 0)
      if (strictlyEval)
        throw new Exception(s"No relevant docs for query")
      else return 0.0

    var sumPrec = 0.0
    var relCount = 0.0
    // Have some relevant - can "properly" evaluate
    for (idx <- result.indices) {
      val so = result(idx)
      if (judgment.isRelevant(so.name)) {
        relCount += 1.0
        sumPrec += relCount / (idx+1)
      }
    }
    sumPrec / judgment.numRel
  }
}
