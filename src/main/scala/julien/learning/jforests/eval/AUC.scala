package julien
package learning
package jforests
package eval

class AUC extends EvaluationMetric(true) {
  case class Pair(key: Double, value: Double)
  implicit object PairOrdering extends Ordering[Pair] {
    def compare(p1: Pair, p2: Pair): Int = p1.key compare p2.key
  }

  // TODO : This can probably be optimized using partitioning of the
  // pair array
  def measure(predictions: Array[Double], sample: Sample): Double = {
    val probs = sample.zipWithIndex.map((s, i) => Pair(predictions(i), s(i)))
    val totalPositive = probs.filter(_.value != 0).size
    val totalNegative = probs.size - totalPositive
    val sortedProbs = probs.sort

    var fp, tp, fpPrev, tpPrev, area = 0.0
    var fPrev = Double.MIN_VALUE
    for (pair <- sortedProbs) {
      val curF = pair.key
      if (curF != fPrev) {
        area += abs(fp - fpPrev) * ((tp + tpPrev) / 2.0)
        fPrev = curF
        fpPrev = fp
        tpPrev = tp
      }
      label = pair.value
      if (label == +1)
        tp += 1
      else
        fp += 1
    }
    area += abs(totalNegative - fpPrev) * ((totalPositive + tpPrev) / 2.0)
    area /= totalPositive.toDouble * totalNegative
    return area
  }
}
