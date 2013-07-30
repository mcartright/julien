package julien
package retrieval

import julien.behavior.{Bounded,Movable,Driven}

object TermCount {
  def apply(psv: PositionStatsView): TermCount = apply(psv, psv)
  def apply(s: String, sv: StatisticsView)(implicit i: Index): TermCount =
    apply(Term(s)(i), sv)
  def apply(c: CountView, s: StatisticsView) = if (c.isInstanceOf[Movable])
    new TermCount(c, s) with Driven { val driver = c.asInstanceOf[Movable] }
  else new TermCount(c, s)
}

/** Echoes the unnormalized term count. A good case against views, actually. */
class TermCount(val op: CountView, val statsrc: StatisticsView)
    extends ScalarWeightedFeature
    with Bounded {
  lazy val children: Array[Operator] = Set[Operator](op).toArray
  lazy val views: Set[View] = Set[View](op)
  override val lowerBound: Double = 0.0
  override val upperBound: Double = statsrc.statistics.max.toDouble
  def eval(id: Int): Double = score(op.count(id))
  def score(c: Int): Double = c.toDouble
}
