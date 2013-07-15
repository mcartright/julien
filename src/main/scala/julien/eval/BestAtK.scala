package julien
package eval

/** Returnsd the highest label found by rank k. */
class BestAtK(docsRetrieved: Int = Int.MaxValue)
    extends QueryEvaluator {
  val relRet = new CountRelevantRetrieved(docsRetrieved)

  def eval[T <: ScoredObject](
    result: QueryResult[T],
    judgments: QueryJudgments,
    strictlyEval: Boolean
  ): Double = {
    val toK = if (docsRetrieved > result.length) result
    else result.take(docsRetrieved)
    val maxLabel = toK.map(so => judgments(so.name)).max
    return maxLabel
  }

  val name: String = s"Best at $docsRetrieved"
}
