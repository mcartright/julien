package julien
package eval

class MeanAveragePrecision extends QueryEvaluator() {

  //val name = "Average Precision"

  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgments,
    strictlyEval: Boolean): Double = {
    if (numRelevant(judgment) == 0)
      if (strictlyEval)
        throw new Exception(s"No relevant docs for query")
      else return 0.0

    var sumPrec = 0.0
    var relCount = 0.0
    // Have some relevant - can "properly" evaluate
    for (idx <- result.indices) {
      val so = result(idx)
      if (isRelevant(so.name, judgment)) {
        relCount += 1.0
        sumPrec += relCount / (idx+1)
      }
    }
    sumPrec / numRelevant(judgment)
  }

  val name: String = "Mean Average Precision"
}
