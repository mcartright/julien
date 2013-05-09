package julien
package learning
package neuralnet

import scala.collection.mutable._

object Neuron {
  def apply(
    learningRate: Double = 0.001,
    tfunc: TransferFunction = new LogiFunction())
  ) = new StandardNeuron(learningRate, tfunc)
}

sealed abstract class Neuron(val learningRate: Double) {
  // Structures to hold in place
  val inLinks = ArrayBuffer[Synapse]()
  val outLinks = ArrayBuffer[Synapse]()
  def output: Double
}

/** Encodes the bias in a network - always produces a 1.0 */
final class BiasNeuron(lr: Double) extends Neuron(lr) {
  val output = 1.0
}

abstract class ComputedNeuron(val lr: Double, val g: TransferFunction)
  extends Neuron(lr)

/** Used in the input layer. The features placed in these
  * neurons are currently not normalized for propagation. I think
  * how to do that is still an open discussion, but if/when it's
  * implemented, we should do it here.
  */
class FeatureNeuron(
  val feature: FeatureOp
  val lr: Double = 0.001,
  val tf: TransferFunction = new LogiFunction()
) extends ComputedNeuron(lr, tf) {
  def output: Double = feature.eval
}

class StandardNeuron(val lr: Double, val tf: TransferFunction)
    extends ComputedNeuron(lr, tf) {
  def output: Double = {
    val wsum = inLinks.map(s => s.source.output * s.weight).sum
    g.compute(wsum)
  }
}
