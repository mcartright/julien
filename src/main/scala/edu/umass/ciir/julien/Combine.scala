package edu.umass.ciir.julien

object Combine {
  def apply(ops: FeatureOp*) = new Combine(ops: _*)
}

class Combine(val ops: FeatureOp*) extends IntrinsicEvaluator {
  lazy val children: Seq[Operator] = List[Operator](ops: _*)
  def views: Set[ViewOp] =
    ops.foldLeft(Set[ViewOp]()) { (s, op) => s ++ op.views }
  def eval : Score = ops.foldLeft(new Score(0.0)) { (sc, op) =>
    op match {
      case i: IntrinsicEvaluator => sc + i.eval
      case _ => sc  // NEED TO COMPLETE THIS
    }
  }
}
