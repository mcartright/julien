package edu.umass.ciir.julien

object Combine {
  def apply(ops: FeatureOp*) = new Combine(ops)
}

class Combine(val ops: Seq[FeatureOp]) extends IntrinsicEvaluator {
  lazy val children: Seq[Operator] = ops
  def views: Set[ViewOp] =
    ops.foldLeft(Set[ViewOp]()) { (s, op) => s ++ op.views }
  def eval : Score = new Score(0.0)
}
