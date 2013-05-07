package julien
package eval
package compare

class CountBetter extends QuerySetComparator {
  def eval(baseline: Array[Double], treatment: Array[Double]): Double = {
    val (wantHigher, wantLower) =
      if (higherIsBetter) (treatment, baseline)
      else (baseline, treatment)

    wantLower.view.zip(wantHigher).map { case (l, h) =>
        if (l < h) 1
        else 0
    }.sum
  }
}
