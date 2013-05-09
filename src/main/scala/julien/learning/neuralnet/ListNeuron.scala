package julien
package learning
package neuralnet

import scala.math._

class ListNeuron extends Neuron {
  var d1: Array.empty[Double]
  var d2: Array.empty[Double]

  override def computeDelta(p: PropParam) {
    val sumLabelExp = param.labels.take(outputs.size).map(v => exp(v)).sum
    d1 = param.labels.take(outputs.size).map(v => exp(v)/sumLabelExp)

    val sumScoreExp = outputs.map(v => exp(v)).sum
    d2 = outputs.map(v => exp(v) / sumScoreExp)
  }

  override def updateWeight(p: PropParam) {
    for (s <- inLinks) {
      val dw =
        d1.zipWithIndex.map((v, idx) => (v - d2(idx)) * s.source.output(idx))
      s.weight += dw * learningRate
    }
  }
}
