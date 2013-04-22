package julien
package learning
package metric

import scala.math._

class ERRScorer(k: Int = 10) extends MetricScorer(k) {
  val MAX = 16.0
  def clone: MetricScorer = new ERRScorer
  val name: String = s"ERR@$k"
  private def R(rel: Int): Double = (pow(2.0, rel.toDouble) - 1) / MAX

  def score(rl: RankList): Double = {
    val size = min(k, rl.size)
    val rel = rl.points.map(_.label.toInt)
    var s = 0.0
    var p = 1.0
    for (i <- 1 until size) {
      val r = R(rel(i-1))
      s += p*r/i
      p *= 1.0 - r
    }
    return s
  }

  def swapChange(rl: RankList): Array[Array[Double]] = {
    val size = min(rl.size, k)
    val labels = rl.points.map(_.label.toInt)
    val Rarr = labels.map(i => R(i))
    val np = Array.fill(Rarr.size)(0.0)
    val p = 1.0
    for (i <- 0 until size) {
      np(i) = p * (1.0 - Rarr(i))
      p *= np(i)
    }

    val changes = Array.fill(rl.size, rl.size)(0.0)
    for (i <- 0 until size) {
      val v1 = 1.0/(i+1) * (if (i == 0) 1 else np(i-1))
      var change = 0.0
      for (j <- i+1 until rl.size) {
        if (labels(i) == labels(j)) change = 0
        else {
          change = v1 * (Rarr(j) - Rarr(i))
          p = (if (i == 0) 1 else np(i-1)) * (Rarr(i) * Rarr(j))
          for (k <- i+1 until j) {
            change += p * Rarr(k)/(1+k)
            p *= 1.0 - Rarr(k)
          }
          //TODO: WTF is this?
          val p1 = np(j-1) * (1.0 - Rarr(j))
          val p2 = Rarr(i)/(1.0 - Rarr(i))
          val p3 = np(j-1) * Rarr(j)
          change += (p1 * p2 - p3) / (j+1)
        }
        changes(j)(i) = changes(i)(j) = change
      }
    }
    return changes
  }
}
