package julien
package retrieval

object TF { def apply(c: CountView, l: LengthsView) = new TF(c, l) }

/** Simple Term Frequency (# times the term occurs in D / |D|) feature. */
class TF(val op: CountView, val lengths: LengthsView)
    extends ScalarWeightedFeature {
  lazy val children: Seq[Operator] = Set[Operator](op, lengths).toList
  lazy val views: Set[View] = Set[View](op, lengths)
  override val upperBound: Double = 1.0
  override val lowerBound: Double = 0.0
  def eval(id: InternalId): Double = score(op.count(id), lengths.length(id))
  def score(c: Int, l: Int): Double = c.toDouble / l.toDouble
}
