package julien
package learning
package neuralnet

import scala.collection.mutable._

class Neuron {
  val momentum = 0.9
  val learningRate = 0.001
  val tfunc: TransferFunction = new LogiFunction()
  val inLinks = ArrayBuffer[Synapse]()
  val outLinks = ArrayBuffer[Synapse]()
  val outputs = ArrayBuffer[Double]()
  val deltas_j = ArrayBuffer[Double]()
  var output = 0.0
  var delta_i = 0.0

  def clearOutputs: Unit = outputs.clear
  def computeOutput: Unit = {
    val wsum = inLinks.map(s => s.source.output * s.weight).sum
    output = tfunc.compute(wsum)
  }

  def computeOutput(i: Int): Unit = {
    val wsum = inLinks.map(s => s.source.output(i) * s.weight).sum
    outputs += tfunc.compute(wsum)
  }

  def computeDelta(p: PropParam): Unit = {
    val pMap = param.pairMap
    val curr = param.current
    delta_i = 0.0
    deltas_j.clear
    for (k <- 0 until pMap(curr).length; i = pMap(curr)(k)) {
      val weight =
        if (param.pairWeight == null) 1
        else param.pairWeight(curr)(k)
      val pij = if (param.pairWeight == null)
        1.0 / (1.0 + exp(outputs(curr)-outputs(j)))
      else { // TODO: LambdaRank variant. Gross this way.
        val left = param.targetValue(curr)(k)
        val right = 1.0 / (1.0 + exp(-(outputs(curr)-outputs(j))))
        left - right
      }
      val lambda = weight * pij
      delta_i += lambda
      deltas_j(k) = lambda * tfunc.computeDerivative(outputs(j))
    }
    delta_i *= tfunc.computeDerivative(outputs(curr))
  }

  def updateWeight(p: PropParam): Unit = {
    for (k <- 0 until inLinks.size; s = inLinks(k)) {
      var sum_j = 0.0
      for (l <- 0 until deltas_j.length) {
        sum_j += deltas_j(l) * s.source.output(p.pairMap(p.current)(l))
        val dw = learningRate * (delta_i + s.source.output(p.current) - sum_j)
        s.weightAdj = dw
        s.updateWeight
      }
    }
  }

  def updateDelta(p: PropParam): Unit = {
    val pMap = param.pairMap
    val pWeight = param.pairWeight
    val curr = param.current
    delta_i = 0.0
    deltas_j.clear
    for (k <- 0 until pMap(curr).length; j = pMap(curr)(k)) {
      val weight = if (pWeight == null) 1.0f else pWeight(curr)(k)
      var errorSum = 0.0
      for (l <- 0 until outlinks.size; s = outLinks(l)) {
        errorSum += s.target.deltas_j(k) * s.weight
        if (k == 0) delta_i += s.target.delta_i * s.weight
      }
      if (k == 0) delta_i *= weight * tfunc.computeDerivative(outputs(curr))
      deltas_j(k) = errorSum * weight * tfunc.computeDerivative(outputs(curr))
    }
  }
}
