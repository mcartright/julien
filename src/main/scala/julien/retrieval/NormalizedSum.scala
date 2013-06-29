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

sealed abstract class NormalizedSum(
  var children: Seq[Feature]
) extends Feature {
  def views: Set[View] = this.grab[View].toSet
  override def toString = s"${getClass.getName}"+children.mkString("(",",",")")

  val weightSum: Double = {
    var i = 0
    var sum = 0.0
    while (i < children.length) {
      sum += children(i).weight
      i += 1
    }
    sum
  }

  def eval(id: InternalId) = {
    var sum = 0.0
    var i = 0
    while (i < children.length) {
      sum += children(i).weight * children(i).eval(id)
      i += 1
    }
    sum / weightSum
  }
}

final class ScalarNormedSum(c: Seq[Feature], override var weight: Double)
  extends NormalizedSum(c)
  with ScalarWeightedFeature
  with Distributive {
  def distribute: Seq[Feature] = {
    // Do NOT like this - would rather immutable & copyable children.
    for (op <- children) op match {
      case fwf: FunctionWeightedFeature =>
        fwf.weight = () => (fwf.weight * (this.weight/weightSum))
      case swf: ScalarWeightedFeature =>
        swf.weight = swf.weight * (this.weight/weightSum)
    }
    children
  }

  def setChildren(newChildren: Seq[Feature]): Unit = children = newChildren
}

final class FunctionalNormedSum(
  c: Seq[Feature],
  wf: () => Double
) extends NormalizedSum(c)
  with FunctionWeightedFeature {
  weightFn = Some(wf)
}
