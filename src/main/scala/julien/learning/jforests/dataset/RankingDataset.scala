package julien.learning
package jforests.dataset

class RankingDataset(
  f: Array[Feature],
  t: Array[Double],
  val queryBoundaries: Array[Int],
  val maxDocsPerQuery: Int) extends Dataset(f,t) {
  lazy val numQueries = queryBoundaries.length - 1
}
