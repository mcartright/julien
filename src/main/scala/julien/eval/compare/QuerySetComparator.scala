package julien
package eval
package compare

object QuerySetComparator {
  def apply(name: String): QuerySetComparator = {
    null
  }
}

abstract class QuerySetComparator(val higherIsBetter: Boolean = true) {
  def mean(numbers: Array[Double]): Double = numbers.sum / numbers.length
  def mulitply(numbers: Array[Double], factor: Double): Array[Double] =
    numbers.map(_ * factor)

  def eval(
    baseline: QuerySetEvaluation,
    treatment: QuerySetEvaluation
  ): Double = {
    assume(baseline.keySet == treatment.keySet,
      s"Can't compare non-matching evaluations.")

    val base = Array.ofDim[Double](baseline.size)
    val treat = Array.ofDim[Double](treatment.size)
    for ((k,i) <- baseline.keys.view.zipWithIndex) {
      base(i) = baseline(k)
      treat(i) = treatment(k)
    }
    eval(base, treat)
  }

  def eval(baseline: Array[Double], treatment: Array[Double]): Double
}
