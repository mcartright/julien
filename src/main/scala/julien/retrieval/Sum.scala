package julien
package retrieval

import julien.behavior._

object Sum {
  def apply(children: Seq[Feature]) = new ScalarSum(children, 1.0)
  def apply(children: Seq[Feature], weight: Double) =
    new ScalarSum(children, weight)
  def apply(children: Seq[Feature], weight: () => Double) =
    new FunctionalSum(children, weight)
}

sealed abstract class Sum(val ops: Seq[Feature]) extends Feature {
  lazy val children: Seq[Operator] = ops
  def views: Set[View] =
    ops.foldLeft(Set[View]()) { (s, op) => s ++ op.views }
  override def toString = s"${getClass.getName}"+children.mkString("(",",",")")

  def eval(id: InternalId) = {
    var sum = 0.0
    var i = 0
    while (i < ops.length) {
      sum += ops(i).weight * ops(i).eval(id)
      i += 1
    }
    sum
  }
}

final class ScalarSum(ops: Seq[Feature], override var weight: Double)
  extends Sum(ops)
  with ScalarWeightedFeature
  with Distributive {
  def distribute: (Seq[Feature], (Feature, InternalId, Double) => Double) = {
    // Do NOT like this - would rather immutable & copyable children.
    for (op <- ops) {
      if (op.isInstanceOf[FunctionWeightedFeature]) {
        // Need to rebind the function used for the operator
        val casted = op.asInstanceOf[FunctionWeightedFeature]
        casted.weight = () => (casted.weight * this.weight)
      } else {
        val casted = op.asInstanceOf[ScalarWeightedFeature]
        casted.weight = casted.weight * this.weight
      }
    }
    val operation = (f: Feature, id: InternalId, d: Double) =>
      (f.eval(id) * f.weight) + d
    (ops, operation)
  }
}

final class FunctionalSum(
  ops: Seq[Feature],
  wf: () => Double
) extends Sum(ops)
  with FunctionWeightedFeature {
  weightFn = Some(wf)
}
