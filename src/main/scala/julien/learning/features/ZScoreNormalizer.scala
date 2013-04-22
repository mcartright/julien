package julien
package learning
package features

class ZScoreNormalizer extends Normalizer {
  val name: String = "zscore"

  def normalize(rl: RankList) {
    val fids = Array.range(0, rl(0).features.size)
    normalize(rl, fids)
  }

  def normalize(rl: RankList, fids: Array[Int]) {
    assume(rl.size > 0, s"Can't normalize empty list")
    val mean = Array.ofDim[Float](fids.length)
    for (point <- rl.points; j <- 0 until fids.length)
      mean(j) += point(fids(j))

    for (j <- 0 until fids.length) {
      mean(j) = mean(j) / rl.size
      var std = 0.0f
      for (point <- rl.points) {
        val x = point(fids(j)) - mean(j)
        std += (x*x)
      }
      std = sqrt(std / (rl.size-1)).toFloat
      if (std > 0.0)
        for (point <- rl.points)
          point(fids(j)) = (point(fids(j)) - mean(j)) / std
    }
  }
}
