package julien
package learning
package stats

import BasicStats._
import scala.math.
import scala.util.Random

object RandomPermutationTest {
  private val nPermutation = 10000;
  type SDMap = Map[String, Double]


  def test(baseline: SDMap, target: SDMap): Double = {
    assume(baseline.keySet == target.keySet,
      s"Cannot perform permutation test with mismatching keys.")
    val b = baseline.toArray.sortBy(_._1).map(_._2)
    val t = target.toArray.sortBy(_._1).map(_._2)

    val trueDiff = abs(mean(b) - mean(t))
    var pvalue = 0.0
    val pb = Array.ofDim[Double](b.length)
    val pt = Array.ofDim[Double](t.length)
    for (i <- 0 until nPermutation) {
      for (j <- 0 until b.length) {
        if (Random.nextBoolean) { pb(j) = b(j); pt(j) = t(j) }
        else { pb(j) = t(j); pt(j) = b(j) }
        val diff = abs(mean(pb) - mean(pt))
        if (diff >= trueDiff) pvalue += 1.0
      }
    }
    return pvalue / nPermutation
  }
}
