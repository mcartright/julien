package julien
package eval

class MeanReciprocalRank extends QueryEvaluator() {
  def eval[T <: ScoredObject](
    result: QueryResult[T],
    judgment: QueryJudgments,
    strictlyEval: Boolean): Double = {
    if (numRelevant(judgment) == 0) 0.0 // need to check this
    else {
      val found = result.find(so => isRelevant(so.name, judgment))
      // Need to check this too
      1.0 / (if (found.isDefined) found.get.rank else result.size)
    }
  }

  val name: String = "Mean Reciprocal Rank"
}
