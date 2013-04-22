package julien
package learning

abstract class Ranker(val samples: List[RankList], val features: Array[Int]) {
  var scorer: MetricScorer
  def init: Unit
  def learn: Unit
  def eval(p: DataPoint): Double = -1.0
  def clone: Ranker
  def model: String
  def name: String
}
