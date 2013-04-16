package julien
package retrieval

object Dirichlet {
  private val defMu = 1500D
  val totallyMadeUpValue = 600
  def apply(op: PositionStatsView, l: LengthsView) = {
    new Dirichlet(op, l, op, defMu)
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
  op: CountView,
  lengths: LengthsView,
  statsrc: StatisticsView,
  mu: Double)
    extends FeatureOp {
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

  override lazy val upperBound: Score = {
    val maxtf = statsrc.statistics.max
    score(maxtf, maxtf)
  }

  // this is a filthy estimation - want something better
  // problem is that it's the longest document *not* seen by
  // the underlying view.
  override lazy val lowerBound: Score =
    score(0, Dirichlet.totallyMadeUpValue)

  def eval: Score = score(op.count, lengths.length)
  def score(c: Count, l: Length):Score = {
    val num = c + (mu*cf)
    val den = l + mu
    new Score(scala.math.log(num / den))
  }
}
