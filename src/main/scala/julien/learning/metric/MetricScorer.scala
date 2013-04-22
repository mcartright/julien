package julien
package learning
package metric

// Encapsulates the factory pattern for metric scorers
object MetricScorer {
  import Metric._

  private val map: Map[Metric, MetricScorer] = Map(
    MAP -> APScorer(),
    NDCG -> NDCGScorer(),
    DCG -> DCGScorer(),
    P -> PrecisionScorer(),
    RR -> ReciprocalRankScorer(),
    BEST -> BestAtKScorer(),
    ERR -> ERRScorer())

  def apply(m: Metric): MetricScorer = map(m).clone
  def apply(m: Metric, k: Int): MetricScorer = map(m).clone(k)
  def apply(mStr: String): MetricScorer = {
    val parts = mStr.split("@")
    if (parts.length == 1)
      map(Metric.withName(parts(0))).clone
    else
      map(Metric.withName(parts(0))).clone(parts(1).toInt)
}

abstract class MetricScorer(val k: Int = 10) {
  assume(k>0, s"What is k < 0 supposed to do?")
  def loadExternalRelevanceJudgment(qrelFile: String): Unit
  def score(rl: RankList): Double
  def swapChange(rl: RankList): Array[Array[Double]]
  def clone(k: Int = this.k): MetricScorer
  val name: String = ""

  def score(rls: List[RankList]): Double =
    (rls.map(l => score(l)).sum / rls.size)
}
