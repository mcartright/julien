package julien
package retrieval
package learning
package linear

import julien.retrieval._
import julien.eval._
import scala.math._

object CascadeRank {
  val nIter = 500
  val tolerance = 0.002
  val maxSelCount = 5
  val defaultBeta = 0.1

  def timeQuery[R](block: => R): Tuple2[R, Long] = {
    val t0 = System.currentTimeMillis
    val result = block
    val t1 = System.currentTimeMillis
    (result, t1 - t0)
  }
}


/** This is an implementation of
  * "A cascade ranking model for efficient ranked retrieval" by
  * Wang, Lin, and Metzler in SIGIR 2011.
  *
  * Implementation Notes:
  *
  * - We do not use binning as described in Section 4.4 of the paper.
  *   Binning seemed nice for convenience, but it limits generalizability.
  *
  * - Partial results and costs are cached all the way through. Therefore,
  *   when calculating something like the overall performance of a cascade
  *   up to step t, the cost is the sum of the stage costs, and the evaluation
  *   is calculated on the results of the last added stage.
  */
class CascadeRank(queries: Map[String, String],
  preparer: QueryPreparer,
  judgments: QueryJudgmentSet,
  evaluator: QueryEvaluator,
  index: Index,
  f: Seq[Feature]) {
  import CascadeRank._

  case class WeightedQuery(val qid: String, var weight: Double = 1.0)
  case class CascadeStage(
    val pruner: ResultPruner,
    val feature: Feature,
    var weight: Double = 1.0
  )
  case class CascadeResult(
    val weight: Double,
    val cost: Double,
    val eval: Double
  )

  assume(f.forall(_.isInstanceOf[ScalarWeightedFeature]),
    s"CascadeRank needs scalar-weighted features.")
  val processor = SimpleProcessor()
  val weightedQueries =
    queries.keys.map(qid => WeightedQuery(qid, 1.0 / queries.size))

  def train: Unit = {
    estimateFeatureCosts
    var trainableFeatures = ListBuffer[ScalarWeightedFeature]() ++=
    f.map(_.asInstanceOf[ScalarWeightedFeature])

    // Estimate costs now
    val featureCosts = f.map { feature =>
      val (result, time) = timeQuery {
        processor.runBatch(queries, preparer, acc)
      }
      (feature -> time)
    }.toMap

    while (!trainableFeatures.isEmpty) {
      val (wr, cResults) = selectWeakRanker(trainableFeatures)

      // calculation of alpha_t
      val num = weightedQueries.view.zip(cResults).foldLeft(0.0) {
        case (score, (wq, cr)) =>
          score += (wq.weight / (1.0 - (gamma * cr.cost))) * (1 + cr.eval)
      }
      val denom = weightedQueries.view.zip(cResults).foldLeft(0.0) {
        case (score, (wq, cr)) =>
          score += (wq.weight / (1.0 - (gamma * cr.cost))) * (1 - cr.eval)
      }

      wr.weight = 0.5 * log(num / denom) // alpha_t

      // Consider this H_t done
      cascade += wr
      trainableFeatures -= wr.feature
      updateQueryWeights
    }
  }

  protected def updateQueryWeights: Unit = {
    // Use cached values to calculate the new P_t+1(q_i)
    val queryUpdateValues = weightedQueries.map { wq =>
      exp(-queryData.evals(wq.query)) *
      exp(gamma * queryData.costs(wq.query))
    }
    val sum = queryUpdateValues.sum
    for ((wq, quv) <- weightedQueries.zip(queryUpdateValues)) {
      wq.weight = quv / sum
    }
  }

  protected def selectWeakRanker(
    available: Seq[ScalarWeightedFeature]
  ): Tuple2[ScalarWeightedFeature, CascadeResult] = {
    // For every possible stage (i.e. prune / feature combination) given the
    // current rankings, which one produces the best remaining docs in the least
    // time?
    val stagesAndScores = for (j <- pruners; h <- available) {
      val s = CascadeStage(j, h)
      val results: Array[CascadeResult] = executeStage(s)
      val weightedCosts = results.map(r => r.weight / 1 - (gamma * r.cost))
      val phi = weightedCosts.zip(results).map((wc,r) => wc * r.eval).sum
      val rightside = weightedCosts.sum
      // Tuple below is: score, stage, cached results
      ((phi * phi) - (rightside * rightside), s, results)
    }

    // Find the maximal pair (compare by score), and extract the stage
    val best = stagesAndScores.maxBy(_._1)
    CascadeFeature(best._2, best._3)
  }

  // Need to get the processor API in line with this implementation
  protected def executeStage(stage: CascadeStage): CascadeResult = {
    val results = weightedQueries.map { wq =>
      // For each query, run the pruner, then execute over the remaining
      // docs (make sure you time it), and get the evaluation score.
      val remaining = stage.pruner.prune(ueryData.partialResults(wq.query))
      processor.clear
      processor add stage.feature
      processor add queryData.partialResults(wq.query)
      val (queryResult, time) = timeQuery {
        processor.run
      }
      val cost = featureCosts(stage.feature) * queryResult.length
      val e = evaluator.eval(queryResult, judgments(wq.query))
      CascadeResult(wq.weight, cost, e)
    }
    results
  }

  lazy val pruners: Array[ResultPruner] = {
    Array(new RankCutoff(defaultBeta), new ScoreCutoff(defaultBeta))
  }

  private val myAcc = DefaultAccumulator[ScoredDocument]()
  def acc: Accumulator[ScoredDocument] = {
    myAcc.clear
    myAcc
  }
}

sealed abstract class ResultPruner(val beta: Double) {
  def prune(incoming: QueryResult): QueryResult
}

class RankCutoff(b: Double) extends ResultPruner(beta) {
  def prune(incoming: QueryResult): QueryResult = {
    val resultSize = (incoming.size * (1-beta)).round.toInt
    QueryResult(incoming.take(resultSize))
  }
}

class ScoreCutoff(b: Double) extends ResultPruner(b) {
  def prune(incoming: QueryResult): QueryResult = {
    val scores = incoming.map(_.score)
    val min = scores.min
    val max = scores.max
    val cutoffScore = (beta * (max-min)) + min
    QueryResult(incoming.takeWhile(_.score > cutoffScore))
  }
}

