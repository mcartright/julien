package julien
package eval

import scala.math._
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import gnu.trove.map.hash.TIntDoubleHashMap

class NormalizedDiscountedCumulativeGain(docsRetrieved: Int = Int.MaxValue)
    extends QueryEvaluator {

  def eval[T <: ScoredObject](
    result: QueryResult[T],
    judgment: QueryJudgments,
    strictlyEval: Boolean): Double = {

    val docJudgments = result.map { so =>
      if (judgment(so.name) > 0) judgment(so.name) else 0.0
    }.toArray
    val limit = min(docJudgments.length, docsRetrieved)
    val dcg = getDCG(docJudgments, limit)

    val ideal = judgment.map { j =>
      if (j.label > 0) j.label.toDouble else 0.0
    }.toArray.sorted.reverse
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
      getDCG(gains, limit, idx+1, sum + inc)
    }
  }

  val name: String = s"NDCG @ $docsRetrieved"
}
