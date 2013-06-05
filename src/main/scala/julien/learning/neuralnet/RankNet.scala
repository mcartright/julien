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
  f: Seq[Feature],
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

  val connections = ArrayBuffer[Synapse]()

  // Hook up the network
  for (layerPair <- layers.sliding(2,1)) {
    connections ++= layerPair(0) feedTo layerPair(1)
  }

  // Make the caching structure
  val signalCache = new NetworkSignalCache

  // And it's ready to go.

  def train: Unit = {
    var nIter = 0
    var error = Double.MaxValue

    startMonitoringNetwork
    while (nIter < maxIter && error > threshold) {
      // use micro-batch updating:

      // For a given query:
      for ((qid, text) <- queries) {
        // run the net against every document, produce a score
        val qresult = processAndCacheNetworkSignals(qid)
        // generate lambda_abs from the QJs and QRs
        // aggregate to lambda_as for each query
        // (need to use pairs and inverses)
        val lambdaUpdates = generateLambdas(qresult, judgments(qid))

        // Sum over the lambdas to perform weight updates - use
        // the gradients you derived before.
        applyLambdaUpdates(lambdaUpdates)
      }

      // You also need to cache all the acivations, inputs, and sums for each
      // synapse/query pair (check gradient functions for args).
    }
  }

  def processAndCacheNetworkSignals(qid: String) : QueryResult {
    // Need some way to just have the nodes be there.
    val query = queries(qid)
    v
  }

  def generateLambdas(
    qResult: QueryResult,
    judgment: QueryJudgment): Map[String, LambdaUpdate] = {
  }

  def applyLambdaUpdates(lambdas: Map[String, LambdaUpdate]): Unit = {
  }

  def startMonitoringNetwork: Unit {

  }

  /** Internal class to aid in caching signals needed for later.
    * Values are added via publisher/subscription service.
    */
  class NetworkSignalCache extends Subscriber[NetworkEvent, Neuron] {
  }
}

