package julien
package retrieval

import julien.behavior.{Bounded,Driven,Movable}

object AbsoluteDiscount {
  private val defDelta = 0.7
  val totallyMadeUpValue = 600

  def apply(
    op: CountStatsView,
    l: LengthsView,
    d: DocumentView
  ): AbsoluteDiscount = apply(op, l, op, d)

  def apply(
    op: CountView,
    l: LengthsView,
    s: StatisticsView,
    d: DocumentView,
    delta: Double = defDelta
  ): AbsoluteDiscount = if (op.isInstanceOf[Movable])
    new AbsoluteDiscount(op, l, s, d, delta) with Driven {
      val driver = op.asInstanceOf[Movable]
    }
    else
      new AbsoluteDiscount(op, l, s, d, delta)
}

class AbsoluteDiscount(
  val op: CountView,
  val lengths: LengthsView,
  val statsrc: StatisticsView,
  val docsrc: DocumentView,
  val delta: Double)
    extends ScalarWeightedFeature
    with Bounded {
  require(delta > 0, s"Delta must be positive. Received $delta")
  lazy val children: Array[Operator] =
    Set[Operator](op, lengths, statsrc, docsrc).toArray
  lazy val views: Set[View] = Set[View](op, lengths, statsrc, docsrc)
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
    score(maxtf, maxtf, 1.0)
  }

  // this is a filthy estimation - want something better
  // problem is that it's the longest document *not* seen by
  // the underlying view.
  override lazy val lowerBound: Double =
    score(0, AbsoluteDiscount.totallyMadeUpValue,
      1.0 / AbsoluteDiscount.totallyMadeUpValue
    )

  def eval(id: InternalId): Double = {
    val doc = docsrc.data(id)
    val ratio = doc.vocabulary.size.toDouble / doc.termVector.size
    score(op.count(id), lengths.length(id), ratio)
  }
  def score(c: Int, l: Int, ratio: Double): Double = {
    val foreground = scala.math.max(c.toDouble - delta, 0.0) / l.toDouble
    val background = delta * ratio * cf
    scala.math.log(foreground + background)
  }
}
