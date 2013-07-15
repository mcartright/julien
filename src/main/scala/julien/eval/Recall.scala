package julien
package eval

class Recall(docsRetrieved: Int = Int.MaxValue)
    extends QueryEvaluator {
  val relRet = new CountRelevantRetrieved(docsRetrieved)

  def eval[T <: ScoredObject](
    result: QueryResult[T],
    judgment: QueryJudgments,
    strictlyEval: Boolean): Double =
    relRet.eval(result, judgment) / numRelevant(judgment)

  val name: String = "Recall"
}
