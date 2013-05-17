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

  val summer: Combiner = (id: InternalId, sops: Seq[FeatureOp]) => {
    // Inlined sum is faster than method call (i.e. 'foldLeft')
    var sum = 0.0
    var i = 0
    while (i < sops.length) {
      sum += sops(i).weight * sops(i).eval(id)
      i += 1
    }
    sum
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
  def eval(id: InternalId) : Double = combiner(id, ops)
}
