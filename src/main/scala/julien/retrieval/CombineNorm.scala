package julien
package retrieval

object CombineNorm {
  def apply(children: Seq[FeatureOp]) =
    new CombineNorm(children, () => 1.0)

  def apply(children: Seq[FeatureOp], weight: Double) =
    new CombineNorm(children, () => weight)

  def apply(children: Seq[FeatureOp], weight: () => Double) =
    new CombineNorm(children, weight)
}

class CombineNorm private(val ops: Seq[FeatureOp],
                          w: () => Double)
  extends FunctionWeightedFeature {
  this.weight = w
  lazy val children: Seq[Operator] = ops
  lazy val localChildren : Array[FeatureOp] = ops.toArray
  def views: Set[ViewOp] =
    ops.foldLeft(Set[ViewOp]()) { (s, op) => s ++ op.views }
  def eval : Double = combine()

  lazy final val childrenSize = ops.size

  val weightSum : Double = {
    var i = 0
    var sum = 0.0
    while (i < childrenSize) {
      val op = ops(i)
      sum += op.weight
      i+=1
    }
    sum
  }

  def combine() : Double = {
    var i = 0
    var score = 0.0
    while (i < childrenSize) {
      score += localChildren(i).eval
      i+=1
    }
    score / weightSum
  }

}


