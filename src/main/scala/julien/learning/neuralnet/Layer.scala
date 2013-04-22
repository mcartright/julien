package julien
package learning
package neuralnet

class Layer(size: Int, isList: Boolean = false) {
  val neurons = Array.fill(size,
    () => if (isList) ListNeuron() else Neuron()
  )
  def apply(i: Int) = neurons(i)
  def size: = neurons.size
  def computeOutput(i: Int): Unit = neurons.foreach(_.computeOutput(i))
  def computeOutput: Unit = neurons.foreach(_.computeOutput)
  def clearOutputs: Unit = neurons.foreach(_.clearOutputs)
  def computeDelta(p : PropParam): Unit = neurons.foreach(_.computeDelta(p))
  def updateDelta(p: PropParam): Unit = neurons.foreach(_.updateDelta(p))
  def updateWeight(p: PropParam): Unit = neurons.foreach(_.updateWeight(p))
}
