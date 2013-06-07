package julien
package retrieval

import julien.behavior._

object NormalizedSum {
  def apply(children: Seq[Feature]) = new ScalarNormedSum(children, 1.0)
  def apply(children: Seq[Feature], weight: Double) =
    new ScalarNormedSum(children, weight)
  def apply(children: Seq[Feature], weight: () => Double) =
    new FunctionalNormedSum(children, weight)
}

sealed abstract class NormalizedSum(val ops: Seq[Feature]) extends Feature {
  lazy val children: Seq[Operator] = ops
  def views: Set[View] =
    ops.foldLeft(Set[View]()) { (s, op) => s ++ op.views }
  override def toString = s"${getClass.getName}"+children.mkString("(",",",")")

  val weightSum: Double = {
    var i = 0
    var sum = 0.0
    while (i < ops.length) {
      sum += ops(i).weight
      i += 1
    }
    sum
  }

  def eval(id: InternalId) = {
    var sum = 0.0
    var i = 0
    while (i < ops.length) {
      sum += ops(i).weight * ops(i).eval(id)
      i += 1
    }
    sum / weightSum
  }
}

final class ScalarNormedSum(ops: Seq[Feature], override var weight: Double)
  extends NormalizedSum(ops)
  with ScalarWeightedFeature
  with Distributive {
  def distribute: (Seq[Feature], (Feature, InternalId, Double) => Double) = {
    // Do NOT like this - would rather immutable & copyable children.
    for (op <- ops) op match {
      case fwf: FunctionWeightedFeature =>
        fwf.weight = () => (fwf.weight * (this.weight/weightSum))
      case swf: ScalarWeightedFeature =>
        swf.weight = swf.weight * (this.weight/weightSum)
    }
    val operation =
      (f: Feature, id: InternalId, d: Double) => (f.eval(id) * f.weight) + d
    (ops, operation)
  }
}

final class FunctionalNormedSum(
  ops: Seq[Feature],
  wf: () => Double
) extends NormalizedSum(ops)
  with FunctionWeightedFeature {
  weightFn = Some(wf)
}
