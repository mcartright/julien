package julien
package eval

import scala.math._

class AreaUnderCurve extends QueryEvaluator() {
  case class Pair(key: Double, value: Double) extends Ordered[Pair] {
    // natural orderingo is "higher key == better"
    def compare(other: Pair): Int =
      if (key > other.key) return -1
      else if (key < other.key) return 1
      else return 0
  }

  // TODO : This can probably be optimized using partitioning of the
  // pair array
  def eval[T <: ScoredObject[T]](
    predictions: QueryResult[T],
    actual: QueryJudgments,
    strictlyEval: Boolean
  ): Double = {
    val ps = predictions.map(_.name).toSet
    val probs = actual.map { case (k,j) =>
      if (ps(j.name)) Pair(1.0, j.label) else Pair(0.0, j.label)
    }
    val totalPositive = numRelevant(actual)
    val totalNegative = numNonRelevant(actual)
    val sortedProbs = probs.toSeq.sorted

    var fp, tp, fpPrev, tpPrev, area = 0.0
    var fPrev = Double.MinValue
    // Start with the highest prediction first, and increase downwards
    for (pair <- sortedProbs) {
      val curF = pair.key
      if (curF != fPrev) {
        area += abs(fp - fpPrev) * ((tp + tpPrev) / 2.0)
        fPrev = curF
        fpPrev = fp
        tpPrev = tp
      }
      if (pair.value == 1)
        tp += 1
      else
        fp += 1
    }
    area += abs(totalNegative - fpPrev) * ((totalPositive + tpPrev) / 2.0)
    area /= totalPositive.toDouble * totalNegative
    return area
  }

  val name: String = "Area Under the Curve"
}
