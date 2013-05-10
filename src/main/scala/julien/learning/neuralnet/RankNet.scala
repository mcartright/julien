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
      // use micro-batch updating:

      // For a given query:
      // run the net against every document, produce a score
      // generate lambda_abs from the QJs and QRs
      // aggregate to lambda_as for each query (need to use pairs and inverses)
      // Sum over the lambdas to perform weight updates - use
      // the gradients you derived before.
      //
      // You also need to cache all the acivations, inputs, and sums for each
      // synapse/query pair (check gradient functions for args).
    }
  }
}

