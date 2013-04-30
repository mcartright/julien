package julien
package retrieval

object CombineNorm {
  def apply(children: Seq[FeatureOp]) =
    new CombineNorm(children, () => 1.0, summer)

  def apply(children: Seq[FeatureOp], weight: Double) =
    new CombineNorm(children, () => weight, summer)

  def apply(children: Seq[FeatureOp], weight: Double, combiner: Combiner) =
    new CombineNorm(children, () => weight, combiner)

  def apply(children: Seq[FeatureOp], weight: () => Double) =
    new CombineNorm(children, weight, summer)

  def apply(children: Seq[FeatureOp], combiner: Combiner) =
    new CombineNorm(children, () => 1.0, combiner)

  def apply(
    children: Seq[FeatureOp],
    weight: () => Double,
    combiner: Combiner) = new CombineNorm(children, weight, combiner)

//  val weightSum : Double = (sops: Seq[FeatureOp]) => {
//    sops.foldLeft(0.0) { (score, op) => { score + op.weight} }
//  }

  val summer: Combiner = (sops: Seq[FeatureOp]) => {

    val weightSum = sops.foldLeft(0.0) { (score, op) => score + op.weight }

    sops.foldLeft(0.0) { (score, op) => {
      val normWeight = (op.weight / weightSum)
      val evaluatedScore = normWeight * op.eval

     // debug("COMBINE: " + op.toString + " cur:" + score + " raw:" + op.eval + " normWeight: " + normWeight + " evalScore:" + evaluatedScore )
      score + evaluatedScore
    } }
  }
}

class CombineNorm private(
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


