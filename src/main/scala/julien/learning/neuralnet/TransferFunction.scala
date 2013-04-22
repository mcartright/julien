package julien
package learning
package neuralnet

trait TransferFunction {
  def compute(x: Double): Double
  def computeDerivative(x: Double): Double
}
