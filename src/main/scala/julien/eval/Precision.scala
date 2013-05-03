package julien
package eval

class Precision(n: String, docsRetrieved: Int = Int.MaxValue)
    extends QueryEvaluator(n) {
  val relRetrieved = new CountRelevantRetrieved(docsRetrieved)

  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    strictlyEval: Boolean): Double = {
    relRetrieved.eval(result, judgment) / docsRetrieved.toDouble
  }
}
