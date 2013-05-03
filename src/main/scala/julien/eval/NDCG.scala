package julien
package eval

import scala.math._
import gnu.trove.map.hash.TIntDoubleHashMap

class NDCG(n: String, docsRetrieved: Int.MaxValue) extends QueryEvaluator(n) {
  def this(numRet: Int) = this(s"NDCG@$numRet", numRet)

  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    strictlyEval: Boolean): Double = {

    val docJudgments = result.map { so =>
      if (judgments(so.name) > 0) judgments(so.name) else 0.0
    }.toArray
    val limit = min(docJudgments.length, docRetrieved)
    val dcg = getDCG(docJudgments, limit)

    val ideal = judgments.map { j =>
      if (j.label > 0) j.label else 0
    }.sort.reverse
    val normalizer = getDCG(ideal, limit)
    return dcg / normalizer
  }

  val logs = new TIntDoubleHashMap()

  @inline
  private def getLog(idx: Int) : Double = {
    if (logs.containsKey(idx)) logs.get(idx)
    else {
      val v = log(idx + 2)
      logs.put(idx, v)
      v
    }
  }

  @tailrec
  private def getDCG(
    gains: Array[Double],
    limit: Int,
    idx: Int = 0,
    sum : Double = 0
  ) : Double = {
    if (idx == limit) sum else {
      val inc = (pow(2, gains(idx)) - 1.0) / getLog(idx)
    }
  }
}
