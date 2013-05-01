package julien
package retrieval

object Combine {
  def apply(children: Seq[FeatureOp]) =
    new Combine(children, () => 1.0, summer)

  def apply(children: Seq[FeatureOp], weight: Double) =
    new Combine(children, () => weight, summer)

  def apply(children: Seq[FeatureOp], weight: Double, combiner: Combiner) =
    new Combine(children, () => weight, combiner)

  def apply(children: Seq[FeatureOp], weight: () => Double) =
    new Combine(children, weight, summer)

  def apply(children: Seq[FeatureOp], combiner: Combiner) =
    new Combine(children, () => 1.0, combiner)

  def apply(
    children: Seq[FeatureOp],
    weight: () => Double,
    combiner: Combiner) = new Combine(children, weight, combiner)

  val summer: Combiner = (sops: Seq[FeatureOp]) => {
    sops.foldLeft(0.0) { (score, op) =>
      val weightedScore = (op.weight * op.eval)
      score + weightedScore
    }
  }
}

class Combine private(
  val ops: Seq[FeatureOp],
  w: () => Double,
  var combiner: Combiner)
    extends FunctionWeightedFeature {
  this.weight = w
  lazy val children: Seq[Operator] = ops
  def views: Set[ViewOp] =
    ops.foldLeft(Set[ViewOp]()) { (s, op) => s ++ op.views }
  def eval : Double = combiner(ops)
}
