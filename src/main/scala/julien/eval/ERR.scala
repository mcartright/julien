package julien
package eval

class ERR(n: String, docsRetrieved: Int = Int.MaxValue)
    extends QueryEvaluator(n) {
  val maxTrecValue = 4
  def this(numRet: Int) = this(s"ERR@$numRet", numRet)

  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    strictlyEval: Boolean): Double = {
    val relevances = result.map { so =>
      if (judgment(so.name) > 0) judgment(so.name) else 0
    }
    getERR(relevances, min(scores.length, docsRetrieved))
  }

  @tailrec
  def getERR(
    scores: Array[Double],
    limit: Int,
    i: Int = 0,
    score: Double = 0.0,
    decay: Double = 1.0
  ): Double = {
    if (idx >= limit) score
    else {
      val r = pow(2, scores(i) - 1) / pow(2, maxTrecValue)
      getERR(scores, limit, i+1, score + (r * decay / i+1), decay * (1 - r))
    }
  }
}
