package edu.umass.ciir.julien

object Weight {
  def apply(op: FeatureOp, weight: Double) = new Weight(op, weight)
}

class Weight(val op: FeatureOp, val weight: Double) extends IntrinsicEvaluator {
  val w = new Score(weight)
  lazy val children: Seq[Operator] = List[Operator](op)
  def views: Set[ViewOp] = op.views
  def eval : Score = op match {
    case i: IntrinsicEvaluator => w * i.eval
    case _ => w
  }
}