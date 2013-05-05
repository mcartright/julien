package julien
package eval

class Precision(n: String, docsRetrieved: Int = Int.MaxValue)
    extends QueryEvaluator(n) {
  def this(i: Int) = this(s"P@$i", i)
  val relRetrieved = new CountRelevantRetrieved(docsRetrieved)

  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    strictlyEval: Boolean): Double = {
    relRetrieved.eval(result, judgment) / docsRetrieved.toDouble
  }
}
