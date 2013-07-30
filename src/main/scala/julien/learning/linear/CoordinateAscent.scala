package julien
package learning
package linear

import scala.math._
import scala.util.Random
import julien.retrieval._
import julien.retrieval.processor._
import julien.eval._

trait AbstractEvaluation {
  def features: Seq[Feature]
  def run(): Double
}

class QueryEvaluation(
  queries: Map[String, String],
  preparer: QueryPreparer,
  judgments: QueryJudgmentSet,
  evaluator: QueryEvaluator,
  index: Index,
  f: Seq[Feature]) extends AbstractEvaluation {

  // some magic to reuse accumulators
  private val myAcc = DefaultAccumulator[ScoredDocument]()
  def acc: Accumulator[ScoredDocument] = {
    myAcc.clear
    myAcc
  }

  // create a processor
  val scalarFeatures = f.map(_.asInstanceOf[ScalarWeightedFeature]).toArray
  val processor = QueryProcessor(Sum(scalarFeatures))

  // return a list of features on demand
  def features = f

  // get a new score based on any changes to our features
  def run() = {
    evaluator.eval(QueryProcessor.runBatch(queries), judgments)
  }
}

class SimpleVariable extends ScalarWeightedFeature {
  def views = ???
  def children = Seq()
  def eval(id: Int) = scalarWeight
  def score = scalarWeight
}

class Evaluation2D(val targetX: Double, val targetY: Double) extends AbstractEvaluation {
  val x = new SimpleVariable()
  val y = new SimpleVariable()
  def features = Seq(x,y)
  def run() = {
    val xt = (x.score - targetX)
    val yt = (y.score - targetY)
    // paraboloid opening down
    val score = -(xt*xt + yt*yt)

    //println((x.eval, y.eval) + " = "+score)
    score
  }
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
class CoordinateAscent(evaluation: AbstractEvaluation) {
  var tolerance = 0.05
  var stepSize = 0.03
  var maxIterations = 2500

  case class Configuration(val score: Double, val weights: Array[Double])

  // Initialization
  assume(evaluation.features.forall(_.isInstanceOf[ScalarWeightedFeature]),
    s"Coordinate Ascent needs scalar-weighted features.")
  val features = evaluation.features.map(_.asInstanceOf[ScalarWeightedFeature]).toArray

  // running best configuration
  var bestConfig = Configuration(evaluation.run(), features.map(_.weight))

  def train: Unit = {
    Random.shuffle(features.toList).foreach(feature => {
      val bestFromFeature = optimizeFeature(feature, bestConfig)

      if (bestConfig.score < bestFromFeature.score)
        bestConfig = bestFromFeature
    })
  }

  def optimizeFeature(
    feature: ScalarWeightedFeature,
    startConfig: Configuration): Configuration = {
    var currentBest = startConfig
    val fIdx = features.indexOf(feature)

    (0 until maxIterations).foreach( iteration => {
      val (upwards, downwards) = step(features(fIdx), stepSize)

      //println("current score: "+currentBest.score+" alt: ["+upwards+","+downwards+"]")
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

      // update feature weights
      features.indices.foreach(idx => {
        features(idx).weight = currentBest.weights(idx)
      })
    })
    currentBest
  }

  def step(feature: ScalarWeightedFeature, delta: Double): (Double, Double) = {
    val initialWeight = feature.weight

    // try the upwards step
    feature.weight = initialWeight + delta
    val upscore = evaluation.run()

    // and now the downwards step
    feature.weight = initialWeight - delta
    val downscore = evaluation.run()

    // reset weight
    feature.weight = initialWeight
    return (upscore, downscore)
  }
}

