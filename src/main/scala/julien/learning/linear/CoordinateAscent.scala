package julien
package learning
package linear

import scala.math._
import scala.util.Random
import julien.retrieval._
import julien.eval._

object CoordinateAscent {
  val slack = 0.001
  val regularized = false
  val tolerance = 0.001
  val stepSize = 0.05
  val stepScale = 2.0
  val stepBase = 0.05
  val nRestart = 2
  val maxIterations = 25
}

/** Uses a simple coordinate ascent algorithm to tune the weights of the
  * provided feature vector. Uses its own instance of a query processor for
  * now to keep the implementation straightforward. I'm sure this will change
  * as we increase efficiency.
  *
  * Some improvements to make:
  * - regularization (i.e. penalizing for massive changes)
  * - track progress (to avoid repeating a retrieval)
  * - simulated annealing
  */
class CoordinateAscent(
  queries: Map[String, String],
  preparer: QueryPreparer,
  judgments: QueryJudgmentSet,
  evaluator: QueryEvaluator,
  index: Index,
  f: Seq[FeatureOp]) {
  case class Configuration(val score: Double, val weights: Array[Double])
  import CoordinateAscent._

  private val myAcc = DefaultAccumulator[ScoredDocument]()
  def acc: Accumulator[ScoredDocument] = {
    myAcc.clear
    myAcc
  }

  // Initialization
  assume(f.forall(_.isInstanceOf[ScalarWeightedFeature]),
    s"Coordinate Ascent needs scalar-weighted features.")
  val features = f.map(_.asInstanceOf[ScalarWeightedFeature]).toArray
  val processor = SimpleProcessor()
  processor.add(features: _*)
  processor add index
  val initialResults = processor.runBatch(queries, preparer, acc)
  val startScore = evaluator.eval(initialResults, judgments)
  var bestConfig = Configuration(startScore, features.map(_.weight))

  def train: Unit = {
    var featuresToOptimize = Random.shuffle(features.toList)
    while (!featuresToOptimize.isEmpty) {
      var feature = featuresToOptimize.head
      featuresToOptimize = featuresToOptimize.tail
      val bestFromFeature = optimizeFeature(feature, bestConfig)
      if (bestConfig.score < bestFromFeature.score)
        bestConfig = bestFromFeature
    }
  }

  def optimizeFeature(
    f: ScalarWeightedFeature,
    startConfig: Configuration): Configuration = {
    var currentBest = startConfig
    val fIdx = features.indexOf(f)
    while (true) {
      val (upwards, downwards) = step(fIdx, stepSize)

      // Is the change big enough?
      if (abs(upwards - currentBest.score) < tolerance &&
        abs(downwards - currentBest.score) < tolerance)
        return currentBest

      currentBest = if (upwards > downwards) {
        val c = Configuration(upwards, features.map(_.weight))
        c.weights(fIdx) += stepSize
        c
      } else {
        val c = Configuration(downwards, features.map(_.weight))
        c.weights(fIdx) -= stepSize
        c
      }
    }
    currentBest
  }

  def step(fidx: Int, delta: Double): Tuple2[Double, Double] = {
    // try the upwards step
    features(fidx).weight += delta
    val upscore =
      evaluator.eval(processor.runBatch(queries, preparer, acc), judgments)

    // and now the downwards step
    features(fidx).weight -= (2*delta)
    val downscore =
      evaluator.eval(processor.runBatch(queries, preparer, acc), judgments)
    features(fidx).weight += delta
    return (upscore, downscore)
  }
}
