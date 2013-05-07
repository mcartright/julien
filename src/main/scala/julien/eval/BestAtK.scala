package julien
package eval

class BestAtK(n: String, docsRetrieved: Int = Int.MaxValue)
    extends QueryEvaluator(n) {
  val relRet = new CountRelevantRetrieved(docsRetrieved)

  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    strictlyEval: Boolean
  ): Double = {
    val toK = if (docsRetrieved > result.length) result
    else result.take(docsRetrieved)
    val maxLabel = toK.map(so => judgment(so.name)).max
    return maxLabel
  }
}
