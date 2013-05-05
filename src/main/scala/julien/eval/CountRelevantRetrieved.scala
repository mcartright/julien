package julien
package eval

import scala.annotation.tailrec

class CountRelevantRetrieved(n: String) extends QueryEvaluator(n) {
  def this(i: Int) = this(s"rel_ret@$i")

  val docsRetrieved = if (n.contains("@"))
    n.split("@")(1).toInt
  else
    Int.MaxValue

  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    strictlyEval: Boolean): Double = {
    val chosen = if (result.size < docsRetrieved)
      result.take(docsRetrieved)
    else
      result
    countRelevant(result, judgment)
  }

  @tailrec
  private def countRelevant[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    idx: Int = 0,
    count: Int = 0) : Int = {
    if (idx == result.size) count
    else {
      val inc = if (judgment.isRelevant(result(idx).name)) 1 else 0
      countRelevant(result, judgment, idx+1, count + inc)
    }
  }
}
