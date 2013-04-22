package julien
package learning
package neuralnet

import scala.math._

class LogiFunction extends TransferFunction {
  def compute(x: Double): Double =
    1.0 / (1.0 + exp(-x))

  def computeDerivative(x: Double): Double = {
    val y = compute(x)
    y * (1.0 - y)
  }
}
