package julien
package eval

import scala.annotation.tailrec

/** Counts the number of relevant documents that were retrieved. */
class CountRelevantRetrieved(docsRetrieved : Int = Int.MaxValue)
    extends QueryEvaluator {

  def eval[T <: ScoredObject](
    result: QueryResult[T],
    judgment: QueryJudgments,
    strictlyEval: Boolean): Double = {
    val chosen = if (result.size < docsRetrieved)
      result.take(docsRetrieved)
    else
      result
    countRelevant(result, judgment)
  }

  @tailrec
  private def countRelevant[T <: ScoredObject](
    result: QueryResult[T],
    judgment: QueryJudgments,
    idx: Int = 0,
    count: Int = 0) : Int = {
    if (idx == result.size) count
    else {
      val inc = if (isRelevant(result(idx).name, judgment)) 1 else 0
      countRelevant(result, judgment, idx+1, count + inc)
    }
  }

  val name: String = s"Relevant Retrieved @ $docsRetrieved"
}
