package julien
package learning
package neuralnet

import scala.math._

object RankNet {
  // one output node
  val nOutput = 1
  val maxIter = 100
  var threshold = 1.0  /// FIXME - need a real value
}

class RankNet(
  queries: Map[String, String],
  preparer: QueryPreparer,
  judgments: QueryJudgmentSet,
  evaluator: QueryEvaluator,
  index: Index,
  f: Seq[FeatureOp],
) {
  import RankNet._

  val totalPairs = judgments.numPrefPairs

  // Initialization code
  // Construct the layers of the network in order
  val layers = Array.newBuilder +=
  // input layer - the bias node is implicitly added by the factory
  Layer.neurons(f) ++=
  // hidden layers - notice they are of uniform size
  (for (i <- 0 until nHiddenLayer) yield Layer.neurons(nHiddenNodePerLayer)) +=
  // output layer
  Layer.neurons(nOutput).
    result // make it an immutable Array

  // Hooks up the network
  for (layerPair <- layers.sliding(2,1)) {
    layerPair(0) feedTo layerPair(1)
  }

  // And it's ready to go.
  def train: Unit = {
    var nIter = 0
    var error = Double.MaxValue
    while (nIter < maxIter && error > threshold) {
      // for now we do batch-style updating

      // Make a sample from the main set of queries
      val qSample = sampleQueries
      // Do a forward-backward round on each,
      // returning all the activations and gradients

    }
  }
}

