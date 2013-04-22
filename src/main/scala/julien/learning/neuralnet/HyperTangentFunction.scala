package julien
package learning
package neuralnet

import scala.math._

class HyperTangentFunction implements TransferFunction {
  def compute(x: Double): Double = 1.7159 * tanh(x*2.0/3)
  def computeDerivative(x: Double): Double = {
    val y = tanh(x*2.0/3)
    1.7159 * (1.0 - (y*y)) * 2.0 / 3
  }
}
