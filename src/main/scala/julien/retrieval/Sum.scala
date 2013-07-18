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

sealed abstract class Sum(var features: Array[Feature]) extends Feature {
  lazy val children: Array[Operator] = features.map(_.asInstanceOf[Operator])
  def views: Set[View] =
    features.foldLeft(Set[View]()) { (s, op) => s ++ op.views }
  override def toString = s"${getClass.getName}"+features.mkString("(",",",")")

  def eval(id: InternalId) = {
    var sum = 0.0
    var i = 0
    while (i < features.length) {
      sum += features(i).weight * features(i).eval(id)
      i += 1
    }
    sum
  }
}

class ScalarSum(
  c: Array[Feature],
  override var weight: Double
) extends Sum(c)
      with ScalarWeightedFeature

class FunctionalSum(
  c: Array[Feature],
  wf: () => Double
) extends Sum(c)
  with FunctionWeightedFeature {
  weightFn = Some(wf)
}
