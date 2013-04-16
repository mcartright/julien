package julien
package retrieval

object JelinekMercer {
  private val defLambda = 0.3
  def apply(
    op: PositionStatsView,
    l: LengthsView,
    lambda: Double = defLambda
  ): JelinekMercer = new JelinekMercer(op, l, op, lambda)
  def apply(
    c: CountView,
    l: LengthsView,
    s: StatisticsView,
    lambda: Double): JelinekMercer =
    new JelinekMercer(c, l, s, lambda)
}

class JelinekMercer(
  op: CountView,
  lengths: LengthsView,
  statsrc: StatisticsView,
  lambda: Double)
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

  // Crappy estimate. What's better?
  override lazy val lowerBound: Score = score(0, 600)

  def eval: Score = score(op.count, lengths.length)
  def score(c: Count, l: Length) =
    new Score(scala.math.log((lambda*(c/l)) + ((1.0-lambda)*cf)))
}


