package julien
package eval

class Precision(docsRetrieved: Int = Int.MaxValue)
    extends QueryEvaluator {
  val relRetrieved = new CountRelevantRetrieved(docsRetrieved)

  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgments,
    strictlyEval: Boolean): Double = {
    relRetrieved.eval(result, judgment) / docsRetrieved.toDouble
  }

  val name: String = s"P@$docsRetrieved"
}
