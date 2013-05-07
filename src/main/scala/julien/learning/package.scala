package julien

import scala.math._

/** This package is inspired (and somewhat based on) two libraries written in
  * Java:
  * - RankLib, by Van Dang; and
  * - JForests, by Yasser Ganjisaffar
  *
  * Whereas each of the above libraries used their evaluation metrics and
  * data representations, this version is tightly integrated with the the
  * retrieval and eval subpackages of Julien. A quick translation between
  *  the data structures of the other libraries and this one can be:
  *
  * RankLib has :
  * DataPoint - in a rank setting this is a single document
  * along with all the values of the features, and that document's actual
  * ground truth label.
  *
  * RankList - a set of DataPoints which can be ordered according to
  * 1 (or many) of the features, or the label of each point.
  *
  * JForests has :
  *
  * Feature - a class to handle discretization of continuous features.
  *
  * DataSet - holds arrays of features, and an array of labels.
  *
  * Sample - a class to take samples from a full dataset.
  *
  * Histogram - records information about the distribution of values w.r.t.
  * a single feature.
  *
  *
  * Some package features still pending:
  * - feature-level normalization (normalize a feature by all of the
  *   values it takes on)
  */
package object learning {
  type Float2D = Array[Array[Float]]
  type Double2D = Array[Array[Double]]
  type Int2D = Array[Array[Int]]

  val MIN_EXP_POWER = -50
  val EPSILON = 1.4E-45
  val LN2 = scala.math.log(2)
  val WIGGLE = 1E-6

    // To extend Double for approx compares
  implicit class ApproxDouble(d: Double) extends Ordered[Double] {
    def ~(other: Double): Boolean = (d - other < WIGGLE) && (other-d < WIGGLE)
    def compare(other: Double): Int = {
      if (d ~ other) return 0
      else if (d < other) return -1
      else return 1
    }
  }

  @inline def entropyln(n: Double): Double = if (n < 1E-6) 0 else n * log(n)

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
