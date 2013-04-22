package julien
package learning
package neuralnet

import scala.math._

class RankNet(
  samples: List[RankList],
  features: Array[Int],
  useList: Boolean = false
)
    extends Ranker(samples, features) {
  def clone: Ranker = new RankNet
  val name: String = "RankNet"

  // Initialization code
  // Construct the layers of the network in order
  val layers = Array.newBuilder +=
  Layer(nInput+1, useList) ++= // input layer + bias node
  // hidden layers - notice they are of uniform size
  (for (i <- 0 until nHiddenLayer) yield new Layer(nHiddenNodePerLayer)) +=
  Layer(nOutput, useList). // output layer
    result // make it an immutable Array

  @inline def inputLayer = layers.head
  @inline def outputLayer = layers.last

  wire()
  val totalPairs = (for {
    s <- sample
    rl = s.getCorrectRanking
    j <- 0 until rl.size
    k <- j+1 until rl.size
    if (rl(j).label > rl(k).label)
      } yield 1).sum
  if (validationSamples != null) {
    for (i <- 0 until layers.size)
      bestModelOnValidation += ArrayBuffer[Double]()
  }

  // TODO: This should simply take two neurons and not care what the layers are
  def connect(srcLayer: Int, srcNeuron: Int, dstLayer: Int, dstNeuron: Int) =
    new Synapse(layers(srcLayer)(srcNeuron), layers(dstLayer)(dstNeuron))

  def wire: Unit = {
    for (i <- 0 until inputLayer.size()-1; j <- 0 until layers(1).size)
      connect(0, i, 1, j)
    for (
      i <- 1 until layers.size-1;
      j <- 0 until layers(i);
      k <- 0 until layers(i+1)) connect(i, j, i+1, k)

    for (
      i <- 1 until layers.size-1;
      j <- 0 until layers(i)) connect(0, inputLayer.size-1, i, j)
  }

  def addInput(p: DataPoint) {
    inputLayer.init.foreach(_.outputs += p(features(k)))
    inputLayer.last.outputs += 1.0f
  }

  def batchFeedForward(rl: RankList): Array[Array[Int]] = {
    val pMap = Array.ofDim[Array[Double]](rl.size)
    for (i <- 0 until rl.size) {
      addInput(rl(i))
      propagate(i)
      val count =
        (for (j <- 0 until rl.size; if (rl(i).label > rl(j).label)) yield 1).sum
      pMap(i) = Array.ofDim[Double](count)
      var k = 0
      for (j <- 0 until rl.size) {
        if (rl(i).label > rl(j).label) {
          pMap(i)(k) = j
          k += 1
        }
      }
    }
    return pMap
  }

  def propagate(i: Int) = layers.tail.foreach(_.computeOutput(i))
  def batchBackProp(pMap: Array[Array[Int]], pWeight: Array[Array[Float]]) {
    for (i <- 0 until pMap.length; p = PropParam(i, pMap)) {
      outputLayer.computeDelta(p)
      for (j <- (1 until layers.size-2).reverse) layers(j).updateDelta(p)
      outputLayer.updateWeight(p)
      for (j <- (1 until layers.size-2).reverse) layers(j).updateWeight(p)
    }
  }

  def internalReorder(rl: RankList): RankList = rl
  def clearNeuronOutputs: Unit = layers.foreach(_.clearOutputs)
  def computePairWeight(
    pMap: Array[Array[Int]],
    rl: RankList): Array[Array[Float]] = null

  def estimateLoss: Unit = {
    var error = 0.0
    for (rl <- samples) {
      for (k <- 0 until rl.size-1; ol = eval(rl(k)); l <- k+1 until rl.size) {
        if (rl(k).label > rl(l).label) {
          val o2 = eval(rk(l))
          error += crossEntropy(o1, o2, 1.0f)
          if (o1 < o2) misorderedPairs += 1
        }
      }
    }
    lastError = round(error/totalPairs)
  }

  def eval(p: DataPoint): Double = {
    for (k <- 0 until inputLayer.size-1) inputLayer(k).output = p(features(k))
    inputLayer.last.output = 1.0f
    layers.tail.foreach(_.computeOutput)
    outputLayer(0).output
  }

  def learn: Unit = {
    for (i <- 1 until nIter) {
      for (j <- 0 until samples.size) {
        val rl = internalReorder(samples(j))
        val pMap = batchFeedForward(rl)
        val pWeight = computePairWeight(pMap, rl)
        batchBackProp(pMap, pWeight)
        clearNeuronOutputs
      }

      val trainingScore = scorer.score(rank(samples))
      estimateLoss()
      // TODO: Refactor how this is done
      if (i % 1 == 0 && validationSamples != null &&
        score > bestScoreValidationData) {
        bestScoreOnValidationData = score
        saveBestModelOnValidation
      }
  }
}

