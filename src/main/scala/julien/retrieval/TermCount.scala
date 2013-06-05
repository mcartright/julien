package julien
package retrieval

object TermCount {
  def apply(c: CountView) = new TermCount(c)
  def apply(s: String)(implicit i: Index) = new TermCount(Term(s)(i))
}

/** Echoes the unnormalized term count. A good case against views, actually. */
class TermCount(val op: CountView)
    extends ScalarWeightedFeature {
  lazy val children: Seq[Operator] = Set[Operator](op).toList
  lazy val views: Set[View] = Set[View](op)
  override val lowerBound: Double = 0.0
  def eval(id: InternalId): Double = score(op.count(id))
  def score(c: Int): Double = c.toDouble
}
