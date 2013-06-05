package julien
package retrieval

object CombineNorm {
  def apply(children: Seq[Feature]) =
    new CombineNorm(children, () => 1.0)

  def apply(children: Seq[Feature], weight: Double) =
    new CombineNorm(children, () => weight)

  def apply(children: Seq[Feature], weight: () => Double) =
    new CombineNorm(children, weight)
}

class CombineNorm private(val ops: Seq[Feature],
                          w: () => Double)
  extends FunctionWeightedFeature {
  this.weight = w
  lazy val children: Seq[Operator] = ops
  lazy val localChildren : Array[Feature] = ops.toArray
  def views: Set[View] =
    ops.foldLeft(Set[View]()) { (s, op) => s ++ op.views }
  def eval(id: InternalId) : Double = combine(id)

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

  def combine(id: InternalId) : Double = {
    var i = 0
    var score = 0.0
    while (i < childrenSize) {
      score += localChildren(i).eval(id)
      i+=1
    }
    score / weightSum
  }

}


