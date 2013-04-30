package julien
package retrieval

object TermCount {
  def apply(c: CountView) = new TermCount(c)
  def apply(s: String) = new TermCount(Term(s))
}

/** Echoes the unnormalized term count. A good case against views, actually. */
class TermCount(val op: CountView)
    extends ScalarWeightedFeature {
  lazy val children: Seq[Operator] = Set[Operator](op).toList
  lazy val views: Set[ViewOp] = Set[ViewOp](op)
  override val lowerBound: Double = 0.0
  def eval: Double = score(op.count)
  def score(c: Int): Double = c.toDouble
}
