package operators

object Combine {
  def apply(ops: FeatureOp*) = new Combine(ops: _*)
}

class Combine(val ops: FeatureOp*) extends IntrinsicEvaluator {
  def eval : Score = ops.foldLeft(new Score(0.0)) { (sc, op) =>
    op match {
      case i: IntrinsicEvaluator => sc + i.eval
      case _ => sc  // NEED TO COMPLETE THIS
    }
  }
}
