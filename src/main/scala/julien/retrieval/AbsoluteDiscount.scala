package julien
package retrieval

object AbsoluteDiscount {
  private val defDelta = 0.7
  val totallyMadeUpValue = 600
  def apply(op: PositionStatsView, l: LengthsView, d: DocumentView) = {
    new AbsoluteDiscount(op, l, op, d, defDelta)
  }
  def apply(c: CountView, l: LengthsView, s: StatisticsView, d: DocumentView) =
    new AbsoluteDiscount(c, l, s, d, defDelta)
  def apply(
    op: CountView,
    l: LengthsView,
    s: StatisticsView,
    d: DocumentView,
    delta: Double): AbsoluteDiscount = new AbsoluteDiscount(op, l, s, d, delta)
}

class AbsoluteDiscount(
  val op: CountView,
  val lengths: LengthsView,
  val statsrc: StatisticsView,
  val docsrc: DocumentView,
  val delta: Double)
    extends FeatureOp {
  require(delta > 0, s"Delta must be positive. Received $delta")

  lazy val children: Seq[Operator] =
    Set[Operator](op, lengths, statsrc, docsrc).toList
  lazy val views: Set[ViewOp] = Set[ViewOp](op, lengths, statsrc, docsrc)
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
    score(maxtf, maxtf, 1.0)
  }

  // this is a filthy estimation - want something better
  // problem is that it's the longest document *not* seen by
  // the underlying view.
  override lazy val lowerBound: Score =
    score(0, AbsoluteDiscount.totallyMadeUpValue,
      1.0 / AbsoluteDiscount.totallyMadeUpValue
    )

  def eval: Score = {
    val doc = docsrc.data
    val ratio = doc.vocabulary.size.toDouble / doc.termVector.size
    score(op.count, lengths.length, ratio)
  }
  def score(c: Count, l: Length, ratio: Double): Score = {
    val foreground = scala.math.max(c.toDouble - delta, 0.0) / l.toDouble
    val background = delta * ratio * cf
    new Score(scala.math.log(foreground + background))
  }
}
