package operators

object Weight {
  def apply(op: FeatureOp, weight: Double) = new Weight(op, weight)
}

class Weight(val op: FeatureOp, val weight: Double) extends IntrinsicEvaluator {
  val w = new Score(weight)
  def views: Set[ViewOp] = op.views
  def eval : Score = op match {
    case i: IntrinsicEvaluator => w * i.eval
    case _ => w
  }
}
