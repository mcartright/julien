package julien
package eval

import scala.math._
import scala.annotation.tailrec

class ExpectedReciprocalRank(docsRetrieved: Int = Int.MaxValue)
    extends QueryEvaluator {
  val maxTrecValue = 4

  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgments,
    strictlyEval: Boolean): Double = {
    val relevances = result.map { so =>
      if (judgment(so.name) > 0) judgment(so.name).toDouble else 0.0
    }.toArray
    getERR(relevances, min(relevances.length, docsRetrieved))
  }

  @tailrec
  private def getERR(
    scores: Array[Double],
    limit: Int,
    i: Int = 0,
    score: Double = 0.0,
    decay: Double = 1.0
  ): Double = {
    if (i >= limit) score
    else {
      val r = pow(2, scores(i) - 1) / pow(2, maxTrecValue)
      getERR(scores, limit, i+1, score + (r * decay / i+1), decay * (1 - r))
    }
  }

  val name: String = s"Expected Reciprocal Rank @ $docsRetrieved"
}
