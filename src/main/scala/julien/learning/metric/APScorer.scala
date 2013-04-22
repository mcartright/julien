package julien
package learning
package metric

import scala.io.Source

object APScorer {
  def apply: APScorer = new APScorer
}

class APScorer extends MetricScorer(0) {
  val relDocCount =
    scala.collection.mutable.HashMap[String, Int]().withDefaultValue(0)

  override val name = "MAP"
  def clone: MetricScorer = APScorer
  def loadExternalRelevanceJudgment(qrelFile: String) {
    val reader = Source.fromFile(qrelFile).bufferedReader
    val LinePattern = """(\w+) 0 .+ ([0-9])"""".r
    while(reader.ready) {
      reader.readLine match {
        case LinePattern(qid, label) => relDocCount(qid) = countTable(qid) + 1
        case _ => // Nothing
      }
    }
    reader.close
  }

  def score(rl: RankList): Double = {
    var ap = 0.0
    var count = 0
    for ((point, idx) <- rl.points.zipWithIndex; if point.label > 0.0) {
      count += 1
      ap += count.toDouble / (idx+1)
    }
    val rdCount: Int = if (relDocCount.isEmpty) count else relDocCount(rl.id)
    // Return result of this if
    if (rdCount == 0) 0.0 else ap / rdCount
  }

  def swapChange(rl: RankList): Array[Array[Double]] = {
    // Functional programming...because why not.
    @tailrec
    def tailsum(hits: List[Int], sum: Int = 0): List[Int] = hits match {
      case Nil => List.empty
      case head :: tail =>
        if (head == 1) (sum+1) :: tailsum(tail, sum+1)
        else sum :: tailsum(tail, sum)
    }

    val labels = rl.points.map(dp => if (dp.label > 0) 1 else 0)
    val count = labels.filter(_ > 0).size
    val relCount = tailsum(labels)
    val rdCount = if (relDocCount.isEmpty) count else relDocCount(rl.id)
    // makes a square matrix of zeroes
    val changes = Array.fill(rl.size, rl.size)(0.0)
    if (rdCount == 0 || count == 0) return changes
    for (i <- 0 until rl.size; j <- i+1 until rl.size) {
      if (labels(i) != labels(j)) {
        val diff = labels(j) - labels(i)
        val left = (relCount(i)+diff)*labels(j)
        val right = relCount(i)*labels(i)
        change += (left - right).toDouble / (i+1)
        for (k <- i+1 until j-1; if labels(k) > 0) {
          change += (-relCount(j)*diff).toDouble / (j+1)
        }
      }
      changes(j)(i) = changes(i)(j) = change / rdCount
    }
    return changes
  }
}
