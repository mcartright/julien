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

class ScalarSum(
  c: Seq[Feature],
  override var weight: Double
) extends Sum(c)
      with ScalarWeightedFeature

class FunctionalSum(
  c: Seq[Feature],
  wf: () => Double
) extends Sum(c)
  with FunctionWeightedFeature {
  weightFn = Some(wf)
}
