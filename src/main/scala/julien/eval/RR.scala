package julien
package eval

class RR(n: String) extends QueryEvaluator(n) {
  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    strictlyEval: Boolean): Double = {
    if (judgment.numRel == 0) 0.0 // need to check this
    else {
      val found = result.find(so => judgment.isRelevant(so.name))
      // Need to check this too
      1.0 / (if (found.isDefined) found.get.rank else result.size)
    }
  }
}
