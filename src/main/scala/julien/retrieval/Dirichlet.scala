package julien
package retrieval

object Dirichlet {
  private val defMu = 1500D
  val totallyMadeUpValue = 600
  def apply(op: PositionStatsView, l: LengthsView) = {
    new Dirichlet(op, l, op, defMu)
  }

  def apply(op: PositionStatsView, l: LengthsView, mu : Double) = {
    new Dirichlet(op, l, op, mu)
  }

  def apply(c: CountView, l: LengthsView, s: StatisticsView) =
    new Dirichlet(c, l, s, defMu)
  def apply(
    op: CountView,
    l: LengthsView,
    s: StatisticsView,
    mu: Double): Dirichlet = new Dirichlet(op, l, s, mu)
}

class Dirichlet(
  val op: CountView,
  val lengths: LengthsView,
  val statsrc: StatisticsView,
  val mu: Double)
    extends ScalarWeightedFeature {
  require(mu > 0, s"Mu must be positive. Received $mu")

  lazy val children: Seq[Operator] = Set[Operator](op, lengths, statsrc).toList
  lazy val views: Set[ViewOp] = Set[ViewOp](op, lengths, statsrc)
  // Runs when asked for the first time, and runs only once
  lazy val cf = {
    val stats: CountStatistics = statsrc.statistics
    if (stats.collFreq == 0)
      0.5 / stats.collLength
    else
      stats.collFreq.toDouble / stats.collLength
  }

  override lazy val upperBound: Double = {
    val maxtf = statsrc.statistics.max
    score(maxtf, maxtf)
  }

  // this is a filthy estimation - want something better
  // problem is that it's the longest document *not* seen by
  // the underlying view.
  override lazy val lowerBound: Double =
    score(0, Dirichlet.totallyMadeUpValue)

  def eval: Double = score(op.count, lengths.length)
  def score(c: Int, l: Int): Double = {
    val num = c + (mu*cf)
    val den = l + mu
    scala.math.log(num / den)
  }
}
