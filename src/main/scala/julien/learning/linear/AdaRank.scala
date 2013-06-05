package julien
package learning
package linear

import scala.language.implicitConversions
import scala.collection.mutable.ListBuffer
import scala.math._
import julien.retrieval._
import julien.eval._

object AdaRank {
  val nIter = 500
  val tolerance = 0.002
  val maxSelCount = 5

  // Just for wrapping
  implicit def swf2seq(
    f: ScalarWeightedFeature
  ): Seq[ScalarWeightedFeature] = Seq(f)
}

class AdaRank(
  queries: Map[String, String],
  preparer: QueryPreparer,
  judgments: QueryJudgmentSet,
  evaluator: QueryEvaluator,
  index: Index,
  f: Seq[Feature]
) {
  import AdaRank._
  case class WeightedQuery(val qid: String, var weight: Double = 1.0)

  assume(f.forall(_.isInstanceOf[ScalarWeightedFeature]),
    s"AdaRank needs scalar-weighted features.")
  val processor = SimpleProcessor()
  val weightedQueries = queries.keys.map(qid => WeightedQuery(qid))
  var trainedFeatures = ListBuffer[ScalarWeightedFeature]()

  def train: Unit = {
    var trainableFeatures = ListBuffer[ScalarWeightedFeature]() ++=
    f.map(_.asInstanceOf[ScalarWeightedFeature])
    while (!trainableFeatures.isEmpty) {
      val wr = selectWeakRanker(trainableFeatures)
      val wrScores = weightedQueries.map(wq => scoreQuery(wq, wr))
      val num = weightedQueries.view.zip(wrScores).map { case (wq, ws) =>
          wq.weight * (1 + ws)
      }.sum
      val denom = weightedQueries.view.zip(wrScores).map { case (wq, ws) =>
          wq.weight * (1 - ws)
      }.sum
      wr.weight = 0.5 * log(num / denom)  // alpha_t

      // Move it to "trained" status.
      trainedFeatures += wr
      trainableFeatures -= wr

      updateQueryWeights
    }
  }

  private def scoreQuery(wq: WeightedQuery, feats: Seq[ScalarWeightedFeature]): Double = {
    processor.clear
    processor.add(feats: _*)
    evaluator.eval(processor.run(), judgments(wq.qid))
  }

  private def updateQueryWeights: Unit = {
    // Score each query against the current ensemble - ignoring weights
    val rawQueryScores =
      weightedQueries.map(wq => scoreQuery(wq, trainedFeatures))
    // exponentiate, gather the sum, and assign a normalized weight
    // worse scores get more weight due to the negative exponent
    val expQueryScores = rawQueryScores.map(s => exp(-s))
    val total = expQueryScores.sum
    for ((eqs, wq) <- expQueryScores.zip(weightedQueries)) {
      wq.weight = eqs / total
    }
  }

  private def selectWeakRanker(
    available: Seq[ScalarWeightedFeature]
  ): ScalarWeightedFeature = {
    val weightedScoresPerRanker = available.map { feat =>
      val weightedQueryScores = weightedQueries.map { wq =>
        wq.weight * scoreQuery(wq, feat)
      }
      weightedQueryScores.sum
    }
    val bestIdx = weightedScoresPerRanker.view.zipWithIndex.maxBy(_._1)._2

    return available(bestIdx)
  }
}
