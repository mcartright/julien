package julien
package learning
package boosting

class RBWeakRanker(val fid: Int, val threshold: Double) {
  def score(p: DataPoint): Int = if (p(fid) > threshold) 1 else 0
}
