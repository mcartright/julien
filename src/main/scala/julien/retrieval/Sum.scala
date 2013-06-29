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

sealed abstract class Sum(var children: Seq[Feature]) extends Feature {
  def views: Set[View] =
    children.foldLeft(Set[View]()) { (s, op) => s ++ op.views }
  override def toString = s"${getClass.getName}"+children.mkString("(",",",")")

  def eval(id: InternalId) = {
    var sum = 0.0
    var i = 0
    while (i < children.length) {
      sum += children(i).weight * children(i).eval(id)
      i += 1
    }
    sum
  }
}

final class ScalarSum(
  c: Seq[Feature],
  override var weight: Double
) extends Sum(c)
      with ScalarWeightedFeature
      with Distributive {
  def distribute: Seq[Feature] = {
    // Do NOT like this - would rather immutable & copyable children.
    for (op <- children) {
      if (op.isInstanceOf[FunctionWeightedFeature]) {
        // Need to rebind the function used for the operator
        val casted = op.asInstanceOf[FunctionWeightedFeature]
        casted.weight = () => (casted.weight * this.weight)
      } else {
        val casted = op.asInstanceOf[ScalarWeightedFeature]
        casted.weight = casted.weight * this.weight
      }
    }
    children
  }

  def setChildren(newChildren: Seq[Feature]): Unit = children = newChildren
}

final class FunctionalSum(
  c: Seq[Feature],
  wf: () => Double
) extends Sum(c)
  with FunctionWeightedFeature {
  weightFn = Some(wf)
}
