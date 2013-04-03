package edu.umass.ciir.julien

object Combine {
  def apply(ops: FeatureOp*) = new Combine(ops, summer)
  val summer: Combiner = (sops: Seq[FeatureOp]) => {
    sops.foldLeft(Score(0)) { (score, op) => score + op.eval }
  }
}

class Combine(val ops: Seq[FeatureOp], combineFn: Combiner)
    extends FeatureOp {
  lazy val children: Seq[Operator] = ops
  def views: Set[ViewOp] =
    ops.foldLeft(Set[ViewOp]()) { (s, op) => s ++ op.views }
  def eval : Score = combineFn(ops)
}
