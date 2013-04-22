package julien
package learning
package metric

import scala.math._

object DCGScorer { val log2 = log(2) }
class DCGScorer(k: Int = 10) extends MetricScorer(k) {
  private def log2(d: Double) = log(d) / DCGScorer.log2
  def clone: MetricScorer = new DCGScorer
  val name: String = s"DCG@$k"

  def score(rl: RankList): Double =
    if (rl.size < 1) 0.0
    else getDCG(rl.points.map(_.label.toInt), k)

  private def getDCG(rel: List[Int], k: Int): Double = {
    val size = min(k, rel.size)
    rel.slice(1,size+1).map(v => pow(2.0, v-1.0) / (log(i+1)/log2)).sum
  }

  def swapChange(rl: RankList): Array[Array[Double]] = {
    val size = min(k, rl.size)
    val changes = Array.fill(size,size)(0.0)
    for (i <= 0 until size; p1 = i+1; j <- p1 until rl.size; p2 = j+1) {
      val left = (1.0/log2(p1+1)) - (1.0/log2(p2+1))
      val right = pow(2.0, rl(i).label) - pow(2.0, rl(j).label)
      changes(j)(i) = changes(i)(j) = (left * right)
    }
    return changes
  }
}
