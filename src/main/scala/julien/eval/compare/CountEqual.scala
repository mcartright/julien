package julien
package eval
package compare

class CountEqual extends QuerySetComparator {
  def eval(baseline: Array[Double], treatment: Array[Double]): Double = {
    baseline.view.zip(treatment).map { case (l, h) =>
        if (l == h) 1
        else 0
    }.sum
  }
}
