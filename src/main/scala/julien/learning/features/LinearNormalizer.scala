package julien
package learning
package features

import scala.math._

class LinearNormalizer extends Normalizer {
  val name: String = "linear"
  def normalize(rl: RankList) {
    val fids = Array.range(0, rl(0).features.size)
    normalize(rl, fids)
  }

  def normalize(rl: RankList, fids: Array[Int]) {
    assume(rl.size > 0, s"Can't normalize empty list")
    val min = Array.ofDim[Float](fids.length)
    val max = Array.ofDim[Float](fids.length)
    for (point <- rl.points; j <- 0 until fids.length) {
      min(j) = min(min(j), point(fids(j)))
      max(j) = max(max(j), point(fids(j)))
    }

    for (point <- rl.points; j <- 0 until fids.length)
      point(fids(j)) = (point(fids(j)) - min(j)) / (max(j) - min(j))
  }
}
