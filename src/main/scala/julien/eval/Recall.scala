package julien
package eval

class Recall(n: String, docsRetrieved: Int = Int.MaxValue)
    extends QueryEvaluator(n) {
  val relRet = new CountRelevantRetrieved(docsRetrieved)

  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    strictlyEval: Boolean): Double =
    relRet.eval(result, judgment) / judgment.numRel
}
