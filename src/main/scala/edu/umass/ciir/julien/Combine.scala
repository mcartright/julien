package edu.umass.ciir.julien

object Combine {
  type Combiner = (Seq[FeatureOp]) => Score

  def apply(ops: FeatureOp*) = new Combine(ops)
  implicit val summer: Combiner = (sops: Seq[FeatureOp]) => {
    // UGH NEED THE EVALUATOR MODULE
    // TODO
    sops.foldLeft(Score(0)) { (score, op) => score }
  }
}

class Combine(val ops: Seq[FeatureOp])
(implicit combineFn: (Seq[FeatureOp]) => Score)
    extends IntrinsicEvaluator {
  lazy val children: Seq[Operator] = ops
  def views: Set[ViewOp] =
    ops.foldLeft(Set[ViewOp]()) { (s, op) => s ++ op.views }
  def eval : Score = combineFn(ops)
}
