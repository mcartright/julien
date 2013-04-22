package julien
package learning
package neuralnet

import scala.util.Random

class Synapse(val source: Neuron, val target: Neuron) {
  source.outLinks += this
  target.inLinks += this
  var weight = (if Random.nextInt(2) 1 else -1) * Random.nextFloat/10
  var weightAdj = 0.0
}
