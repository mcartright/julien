package julien
package learning
package metric

// TODO: I'm not convinced the indices are correct when selecting the
// set to judge. Talk to Van about that.
class BestAtKScorer extends MetricScorer(10) {
  def clone: MetricScorer = new BestAtKScorer
  val name: String = s"Best@$k"

  def score(rl: RankList): Double = rl(maxToK(rl, k-1)).label

  private def maxToK(rl: RankList, k: Int): Int = {
    val size = min(rl.size-1, k)
    // Make (point, idx) tuples, then fold left, keeping max all the way
    // and return index of max tuple
    rl.points.take(size+1).zipWithIndex.reduceLeft((l, r) =>
      if (l._1 < r._1) r else l)._2
  }
  // TODO: Ask Van about what this should be doing
  def swapChange(rl: RankList): Array[Array[Double]] = {
    return Array.fill(rl.size,rl.size)(0.0)
  }
}
