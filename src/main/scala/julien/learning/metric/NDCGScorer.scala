package julien
package learning
package metric

import scala.io.Source
import scala.collection.mutable.{Hashmap,ListBuffer}
import scala.math._

class NDCGScorer(k:Int = 10) extends MetricScorer(k) {
  val idealGains = HashMap[String, Double]()
  val name: String = s"NDCG@$k"
  def clone(k: Int): MetricScorer = new NDCGScorer(k)
  def loadExternalRelevanceJudgment(qrelFile: String) {
    val qrels =
      HashMap[String,ListBuffer[Int]].withDefault(ListBuffer[Int]())
    val LinePattern = """(\w+) 0 .+ ([0-9])"""".r
    val reader = Source.fromFile(qrelFile).bufferedReader
    while (reader.ready) {
      reader.readLine match {
        case LinePattern(qid, label) => qrels(qid) += label.toInt
        case _ => // Nothing
      }
    }
    reader.close
    for ((key, v) <- qrels) idealGains(key) = getIdealDCG(v, k)
  }

  private def getDCG(rel: ListBuffer[Int], k: Int): Double = {
    val size = min(k, rel.size)
    val dcg = 0.0
    for (i <- 1 to size) {
      dcg += (pow(2.0, rel(i-1))-1.0)/log2(i+1)
    }
    return dcg
  }

  private def getIdealDCG(rel: ListBuffer[Int], k: Int): Double = {
    val size = min(rel.size, k)
    val idx = Sorter.sort(rel, false)
    var dcg = 0.0
    for (i <- 1 to size) {
      dcg += (pow(2.0, rel(idx(i-1)))-1.0)/log2(i+1)
    }
    return dcg
  }

  def score(rl: RankList): Double = {
    if (rl.size == 0) return -1.0
    val rel = rl.points.map(_.label.toInt)
    val d2 = if (idealGains.isEmpty) getIdealDCG(rel, k) else idealGains(rl.id)
    if (d2 <= 0.0) 0.0 else getDCG(rel, k) / d2
  }

  def swapChange(rl: RankList): Array[Array[Double]] = {
    // TODO: Do this stupid check ONCE in the superclass constructor
    val size = min(k, rl.size)
    val rel = rl.points.map(_.label.toInt)
    val d2 = if (idealGains.isEmpty)
      getIdealDCG(rel, size)
    else
      idealGains(rl.id)

    val changes = Array.fill(rl.size, rl.size)(0)
    // TODO: The Java version of this is...just...This needs to be
    // double-checked.
    if (d2 < 0) return changes
    for (i <- 0 until size; p1 = i+1; j <- i+1 until rl.size; p2 = j+1) {
      val left = (1.0/log2(p1+1)) - (1.0/log2(p2+1))
      val right = pow(2.0, rel(i)) - pow(2.0, rel(j))
      changes(j)(i) = changes(i)(j) = left * right / d2
    }
    return changes
  }
}
