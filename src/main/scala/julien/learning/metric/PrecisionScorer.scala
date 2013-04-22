package julien
package learning
package metric

class PrecisionScorer(k: Int = 10) extends MetricScorer(k) {
  def binaryRel(label: float): Int = if (label > 0.0) 1 else 0
  def clone(k: Int): MetricScorer = new PrecisionScorer(k)
  val name: String = s"P@$k"


  def score(rl: RankList): Double = {
    val size = min(k, rl.size)
    val count = rl.points.take(size).map(_.label).filter(_ > 0.0).size
    count.toDouble / rl.size
  }

  def swapChange(rl: RankList): Array[Array[Double]] = {
    val size = min(k, rl.size)
    val relCount = rl.points.map(_.label).filter(_ > 0.0).size
    val changes = Array.fill(rl.size, rl.size)(0.0)
    // TODO: Second index is suspect. Verify.
    for (i <- 0 until size; j <- size until rl.size) {
      changes(i)(j) = changes(j)(i) =
        (binaryRel(rl(j).label) - binaryRel(rl(i).label))
    }
  }
}
