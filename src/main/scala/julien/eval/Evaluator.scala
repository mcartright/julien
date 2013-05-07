package julien
package eval

import julien.galago.tupleflow.Parameters
import scala.collection.JavaConversions._
import collection.mutable.HashMap
import collection.mutable.ListBuffer
import java.io._

/**
 * User: jdalton
 * Date: 4/30/13
 */
object Evaluator {
  type DoubleList = ListBuffer[Double]
  type Judgment = Tuple2[String, Double]

  val metrics = Array("num_ret", "num_rel", "num_rel_ret", "map", /*"gmap",*/
    "R-prec", "bpref", "recip_rank", "ndcg", "ndcg5", "ndcg10", "ndcg20", "ERR",
    "ERR10", "ERR20", "P1", "P5", "P10", "P15", "P20", "P30", "P50", "P100",
    "P200", "P500", "P1000");
  val evalFormat = "%2$-16s%1$3s %3$6.5f"

  def evaluate[T <: ScoredObject[T]](
    qrelFile: String,
    resultMap: Map[String, Seq[T]]
  ): (Seq[Judgment], Map[String, DoubleList]) = {
    val newResults = resultMap.map{ case (n, sos) => (n, QueryResult(sos)) }
    val qsr = QueryResultSet(newResults)
    val qrels = QueryJudgmentSet(qrelFile, true)
    val evaluators = metrics.map( m => QueryEvaluator(m))
    val queryByQueryResults = HashMap[String, DoubleList]()
    val summaryResults = ListBuffer[Judgment]()

    for (query <- qsr.keys; evaluator <- evaluators) {
      val evalScore = evaluator.eval(qsr(query), qrels(query))
      queryByQueryResults.
        getOrElseUpdate(evaluator.metric, ListBuffer()) += evalScore
    }

    evaluators.foreach(evaluator => {
      val summaryScore = evaluator.eval(qsr, qrels)
      summaryResults += ((evaluator.metric, summaryScore))
    })
    (summaryResults.toSeq, queryByQueryResults.toMap)
  }
}
