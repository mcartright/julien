package julien
package learning
package features

class SumNormalizer extends Normalizer {
  val name:String = "sum"
  def normalize(rl: RankList) {
    val fids = Array.range(0, rl(0).features.size)
    normalize(rl, fids)
  }

  def normalize(rl: RankList, fids: Array[Int]) {
    assume(rl.size > 0, s"Can't normalize empty list")
    val norm = Arrays.fill(fids.length)(0)
    for (point <- rl.points; j <- 0 until fids.length)
      norm(j) += scala.math.abs(point(fids(j)))

    for (point <- rl.points; j <- 0 until fids.length)
      point(fids(j)) = points(fids(j)) / norm(j)
  }
}
