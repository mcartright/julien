package julien

import scala.math._

package object eval {
  // These are carried over from the 'aggregate' package of Galago's eval
  def gMean(values: Array[Double]): Double =
    pow(values.product, 1.0 / values.size)

  def aMean(values: Array[Double]): Double = values.sum / values.size
  def mean(values: Array[Double]) = aMean(values)
}
