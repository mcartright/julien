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
  def views: Set[ViewOp] =
    ops.foldLeft(Set[ViewOp]()) { (s, op) => s ++ op.views }
  def eval : Double = combine()

  val weightSum : Double = {

    var i = 0
    var sum = 0.0
    while (i < ops.size) {
      val op = ops(i)
      sum += op.weight
      i+=1
    }
    sum
    //    @tailrec def scoreNormalizeHelper(sops:Seq[FeatureOp], score:Double) : Double = {
    //      sops match {
    //        case Nil => score
    //        case w =>  scoreNormalizeHelper(sops.tail, score + ops.head.weight)
    //      }
    //    }
    // scoreNormalizeHelper(ops, 0.0)
  }

  def combine() : Double = {

    var i = 0
    var score = 0.0
    while (i < ops.size) {
      val op = ops(i)
      score += op.eval / weightSum
      i+=1
    }
    score
  }

}


