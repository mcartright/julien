package julien
package retrieval

import julien.behavior._

object Dirichlet {
  private val defMu = 1500D
  val totallyMadeUpValue = 600

  def apply(op: PositionStatsView, l: LengthsView) = {
    new Dirichlet(op, l, op, defMu, () => 1.0)
  }

  def apply(op: PositionStatsView, l: LengthsView, mu: Double) = {
    new Dirichlet(op, l, op, mu, () => 1.0)
  }

  def apply(op: PositionStatsView,
    l: LengthsView,
    mu: Double,
    weight: Double
  ) = new Dirichlet(op, l, op, mu, () => weight)

  def apply(c: CountView, l: LengthsView, s: StatisticsView) =
    new Dirichlet(c, l, s, defMu, () => 1.0)

  def apply(
    op: CountView,
    l: LengthsView,
    s: StatisticsView,
    mu: Double): Dirichlet = new Dirichlet(op, l, s, mu, () => 1.0)
}

class Dirichlet(
  val op: CountView,
  val lengths: LengthsView,
  val statsrc: StatisticsView,
  val mu: Double,
  w: () => Double)
    extends ScalarWeightedFeature
    with Bounded
{
  require(mu > 0, s"Mu must be positive. Received $mu")
  this.weight = w()
  lazy val children: Seq[Operator] = Set[Operator](op, lengths, statsrc).toList
  lazy val views: Set[View] = Set[View](op, lengths, statsrc)
  // Runs when asked for the first time, and runs only once
  lazy val cf = {
    val stats: CountStatistics = statsrc.statistics
    val cf = if (stats.collFreq == 0) {
      0.5 / stats.collLength
    } else {
      stats.collFreq.toDouble / stats.collLength
    }
    cf
  }

  override lazy val upperBound: Double = {
    val maxtf = statsrc.statistics.max
    score(maxtf, maxtf)
  }

  override lazy val lowerBound: Double = {
    // Not sure which one is less accurate, actually...
    //score(0, statsrc.statistics.longestDoc)
    score(0, Dirichlet.totallyMadeUpValue)
  }

  def eval(id: InternalId): Double = score(op.count(id), lengths.length(id))

  def score(c: Int, l: Int): Double = {
    val num = (c + (mu * cf))
    val den = (l + mu)
    val rawScore = scala.math.log((c + (mu * cf)) / (l + mu))
    scala.math.log((c + (mu * cf)) / (l + mu))
  }
}
