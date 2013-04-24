package julien
package learning
package jforests
package sample

import scala.util.Random

object RankingSample {
  def apply(rd: RankingDataset) = {
    val filler = Array.range(0, rd.size)
    new RankingSample(
      rd,
      Array.range(0, rd.queryBoundaries.length - 1),
      rd.queryBoundaries,
      filler,
      filler,
      dataset.targets,
      Array.empty,
      rd.size,
      rd.queryBoundaries.length-1)
  }

  def apply(rs: RankingSample) = new RankingSample(
    rs.dataset, rs.queryIndices, rs.queryBoundaries,
    rs.indicesInDataset, rs.weights, rs.targets,
    rs.indicesInParentSample, rs.size, rs.numQueries)

  def apply(d: RankingDataset, qi: Array[Int], qb: Array[Int],
  i: Array[Int], w: Array[Double], t: Array[Double], iips: Array[Int],
  dc: Int, nq: Int) = new RankingSample(d, qi, qb, i, w, t, iips, dc, nq)
}

class RankingSample private (
  d: RankingDataset,
  val queryIndices: Array[Int],
  val queryBoundaries: Array[Int],
  i: Array[Int],
  w: Array[Double],
  t: Array[Double],
  iips: Array[Int],
  dc: Int,
  val numQueries: Int)
) extends Sample(d, i, w, t, iips, dc) {
  def getRandomSubSample(rate: Double): RankingSample = {
    assume(rate >= 0.0 && rate <= 1.0, s"got weird sample rate.")
    if (rate == 1.0) {
      RankingSample(this)
    } else {
      val sampleSize = (numQueries * rate)
      val sampleIndices =
        Random.shuffle(Array.range(0, size)).take(sampleSize).sort

    }
  }
}
