package julien
package learning
package stats

object BasicStats {
  def mean(values: Array[Double]): Double = {
    assume(values.length > 0, s"Cannot take mean of empty array.")
    values.sum.toDouble / value.length.toDouble
  }
}
