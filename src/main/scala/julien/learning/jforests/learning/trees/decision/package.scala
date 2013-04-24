package julien.learning
package jforests.learning.trees

package object decision {
  @inline entropyln(n: Double): Double = if (n < 1E-6) 0 else n * log(n)

  def entropy(dist: Array[Double]): Double = {
    val total = dist.sum
    if (total ~ 0) return 0  // about 0
    val numerator = -(dist.map(entropyln(_)).sum)
    (numerator + entropyln(total)) / (total * LN2)
  }

  def entropy(lDist: Array[Double], rDist: Array[Double]): Double = {
    val lTotal = lDist.sum
    val rTotal = rDist.sum
    val total = lTotal + rTotal
    if (total ~ 0) return 0

    val lnum = -(lDist.map(entropyln(_)).sum)
    val rnum = -(rDist.map(entropyln(_)).sum)
    val numerator = lnum + entropyln(lTotal) + rnum + entropyln(rTotal)
    (numerator / (total * LN2))
  }
}
