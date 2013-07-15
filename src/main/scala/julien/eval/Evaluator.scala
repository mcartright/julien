package julien
package eval

import collection.mutable.HashMap
import collection.mutable.ListBuffer

/**
 * User: jdalton
 * Date: 4/30/13
 */
object Evaluator {
  type DoubleList = ListBuffer[Double]
  type Judgment = Tuple2[String, Double]

  val metrics = Array(
    Metric.numRetrieved,
    Metric.numRelevant,
    Metric.relevantRetrieved,
    Metric.meanAveragePrecision,
    Metric.rPrecision,
    Metric.bPref,
    Metric.meanReciprocalRank,
    Metric.normalizedDiscountCumulativeGain(Int.MaxValue),
    Metric.normalizedDiscountCumulativeGain(5),
    Metric.normalizedDiscountCumulativeGain(10),
    Metric.normalizedDiscountCumulativeGain(20),
    Metric.expectedReciprocalRank(Int.MinValue),
    Metric.expectedReciprocalRank(10),
    Metric.expectedReciprocalRank(20),
    Metric.precision(1),
    Metric.precision(5),
    Metric.precision(10),
    Metric.precision(20),
    Metric.precision(30),
    Metric.precision(50),
    Metric.precision(100),
    Metric.precision(200),
    Metric.precision(500),
    Metric.precision(1000)
  )
//  val metrics = Array("num_ret", "num_rel", "num_rel_ret", "map", /*"gmap",*/
//    "R-prec", "bpref", "recip_rank", "ndcg", "ndcg5", "ndcg10", "ndcg20", "ExpectedReciprocalRank",
//    "ERR10", "ERR20", "P1", "P5", "P10", "P15", "P20", "P30", "P50", "P100",
//    "P200", "P500", "P1000");
  val evalFormat = "%2$-16s%1$3s %3$6.5f"

  def evaluate[T <: ScoredObject](
    qrelFile: String,
    resultMap: Map[String, Seq[T]]
  ): (Seq[Judgment], Map[String, DoubleList]) = {
    val newResults = resultMap.map{ case (n, sos) => (n, QueryResult(sos)) }
    val qsr = QueryResultSet(newResults)
    val qrels = QueryJudgmentSet.fromTrec(qrelFile, true)
    val queryByQueryResults = HashMap[String, DoubleList]()
    val summaryResults = ListBuffer[Judgment]()

    for (query <- qsr.keys; evaluator <- metrics) {
      val evalScore = evaluator.eval(qsr(query), qrels(query))
      queryByQueryResults.
        getOrElseUpdate(evaluator.name, ListBuffer()) += evalScore
    }

    metrics.foreach(evaluator => {
      val summaryScore = evaluator.eval(qsr, qrels)
      summaryResults += ((evaluator.name, summaryScore))
    })
    (summaryResults.toSeq, queryByQueryResults.toMap)
  }
}
