package julien
package learning
package metric

class ReciprocalRankScorer(k: Int = 0) extends MetricScorer(k) {
  val name = s"RR@$k"
  def clone(k: Int): MetricScorer = this(k)

  def score(rl: RankList): Double = {
    val first = rl.points.indexWhere(_.label > 0.0)
    if (first == -1) 0 else (1.0F / (first + 1).toFloat)
  }

  def swapChange(rl: RankList): Array[Array[Double]] = {
    val size = scala.math.min(k, rl.size)
    var firstRank = rl.points.indexWhere(_.label > 0.0)
    var secondRank = rl.points.indexWhere(_.label > 0.0, firstRank+1)
    val changes = Array.fill(rl.size, rl.size)(0)
    if (firstRank == -1) firstRank = size
    else {
      val rr = 1.0 / (firstRank+1)
      for (j <- (firstRank+1) until size) {
        if (rl(j).label.toInt == 0) {
          if (secondRank == -1 || j < secondRank)
            changes(firstRank)(j) = changes(j)(firstRank) = (1.0 / (j+1)) - rr
          else
            changes(firstRank)(j) = changes(j)(firstRank) =
              (1.0 / (secondRank+1)) - rr
        }
      }

      for (j <- size until rl.size) {
        if (rl(j).label.toInt == 0) {
          if (secondRank == -1)
            // TODO : Just realized should move the if to the last assignment
            // Should do that all over
            changes(firstRank)(j) = changes(j)(firstRank) = -rr
          else
            changes(firstRank)(j) = changes(j)(firstRank) =
              (1.0 / (secondRank+1)) - rr
        }
      }
    }

    for (i <- 0 until firstRank; j <- firstRank until rl.size) {
      // TODO: Should add a "isRelevant" method or something to DataPoint
      if (rl(j).label.toInt > 0)
        changes(i)(j) = changes(j)(i) = (1.0 / (i+1)) - rr
    }
    return changes
  }
}
