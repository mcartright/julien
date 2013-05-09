package julien
package learning
package neuralnet

import julien.retrieval.FeatureOp

object Layer {
  def neurons(f: Seq[FeatureOp]) = new FeatureLayer(f)
  def neurons(s: Int) = new NeuronLayer(s)
  def lists(s: Int) = new ListLayer(s)
}

sealed abstract class Layer {
  type T <: Neuron
  val neurons: Array[T]
  def apply(i: Int) = neurons(i)
  var bias: Option[BiasNeuron] = None
  def size: = neurons.size
  def feedTo(other: Layer): Unit = {
    for (n1 <- neurons; n2 <- other.neurons) Synapse(n1 ,n2)
    // create a bias neuron for the forward layer
    val b = new BiasNeuron()
    for (n2 <- other.neurons) Synapse(b, n2)
    bias = Some(b)
  }
}

final class FeatureLayer(f: Seq[FeatureOp]) extends Layer {
  type T = FeatureNeuron
  val neurons = f.map(new FeatureNeuron(_)).toArray
}

final class NeuronLayer(sz: Int) extends Layer {
  type T = Neuron
  val neurons = Array.fill(sz, Neuron())
}

final class ListLayer(sz: Int) extends Layer {
  type T = ListNeuron
  val neurons = Array.fill(sz, ListNeuron())
}
