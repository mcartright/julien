package julien
package retrieval

import julien.behavior._

object TF {
  def apply(psv: PositionStatsView, l: LengthsView): TF = apply(psv, psv, l)
  def apply(c: CountView, sv: StatisticsView, l: LengthsView) =
    if (c.isInstanceOf[Movable])
      new TF(c, sv, l) with Driven { val driver = c.asInstanceOf[Movable] }
    else new TF(c, sv, l)
}

/** Simple Term Frequency (# times the term occurs in D / |D|) feature. */
class TF(
  val op: CountView,
  val statsrc: StatisticsView,
  val lengths: LengthsView
)
    extends ScalarWeightedFeature
    with Bounded
{
  lazy val children: Array[Operator] =
    Set[Operator](op, lengths).toArray
  lazy val views: Set[View] = Set[View](op, lengths)
  override val upperBound: Double = statsrc.statistics.max.toDouble
  override val lowerBound: Double = 0.0
  def eval(id: InternalId): Double = score(op.count(id), lengths.length(id))
  def score(c: Int, l: Int): Double = c.toDouble / l.toDouble
}
