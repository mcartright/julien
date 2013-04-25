package julien
package retrieval

object Weight {
  def apply(op: FeatureOp, weight: Double) = new Weight(op, weight)
}

class Weight(val op: FeatureOp, val weight: Double)
    extends FeatureOp {
  val w = weight
  lazy val children: Seq[Operator] = List[Operator](op)
  def views: Set[ViewOp] = op.views
  def eval : Double = weight * op.eval
}
