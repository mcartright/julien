package julien
package learning
package neuralnet

import scala.util.Random

/** Encodes a weighted connection between two Neurons. */
case class Synapse(
  val source: Neuron,
  val target: Neuron,
  var weight: Double = unitGaussian) {
  source.outLinks += this
  target.inLinks += this
}
