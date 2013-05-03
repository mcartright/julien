package julien.cli.examples


import org.lemurproject.galago.tupleflow.Parameters
import scala.collection.JavaConversions._
import julien.eval.aggregate._;
import julien.retrieval.ScoredObject
import collection.mutable.HashMap
import collection.mutable.ListBuffer
import java.io._

/**
 * User: jdalton
 * Date: 4/30/13
 */
object Evaluator {
  type SObj <: ScoredObject[SObj]

  // Can we infer this from the trec result files? Does it need to be static?
  val metrics = Array("num_ret", "num_rel", "num_rel_ret", "map", /*"gmap",*/
    "R-prec", "bpref", "recip_rank", "ndcg", "ndcg5", "ndcg10", "ndcg20", "ERR",
    "ERR10", "ERR20", "P1", "P5", "P10", "P15", "P20", "P30", "P50", "P100",
    "P200", "P500", "P1000");
  val evalFormat = "%2$-16s%1$3s %3$6.5f"

  def evaluate(qrelFile: String, resultMap: Map[String, Seq[SObj]]):
      (Seq[(String, Double)], Map[String, ListBuffer[Double]]) = {
    val newResults = resultMap.map(r => r._1 -> r._2.toArray[SObj])
    val qsr = new QuerySetResults(newResults);
    val qrels = new QuerySetJudgments(qrelFile, true, true)
    val evalParams = new Parameters()

    val evaluators = for (metric <- metrics) yield {
      QuerySetEvaluatorFactory.instance(metric, evalParams)
    }

    val queryByQueryResults = new HashMap[String, ListBuffer[Double]]
    val summaryResults = new ListBuffer[(String, Double)]

    for (query <- qsr.getQueryIterator()) {
      for (evaluator <- evaluators) {
        val evalScore = evaluator.evaluate(qsr.get(query), qrels.get(query))
        queryByQueryResults.
          getOrElseUpdate(evaluator.getMetric(), ListBuffer()) += evalScore
      }
    }

    evaluators.map(evaluator => {
      val summaryScore = evaluator.evaluate(qsr, qrels)
      summaryResults += ((evaluator.getMetric(), summaryScore))
    })
    (summaryResults.toSeq, queryByQueryResults.toMap)
  }
}
