package julien
package retrieval

import julien.behavior._

object NormalizedSum {
  def apply(weight: Double, children: Feature*): NormalizedSum =
    apply(children, weight)
  def apply(children: Seq[Feature]) = new ScalarNormedSum(children, 1.0)
  def apply(children: Seq[Feature], weight: Double) =
    new ScalarNormedSum(children, weight)
  def apply(children: Seq[Feature], weight: () => Double) =
    new FunctionalNormedSum(children, weight)
}

abstract class NormalizedSum(f: Array[Feature]) extends Sum(f) {
  // Normalize the actual weights
  lazy val weightSum: Double = features.map(_.weight).sum

  override def eval(id: Int) = {
    var sum = 0.0
    var i = 0
    while (i < features.length) {
      sum += features(i).weight * features(i).eval(id)
      i += 1
    }
    sum / weightSum
  }
}

class ScalarNormedSum(c: Seq[Feature], override var weight: Double)
  extends NormalizedSum(c)
  with ScalarWeightedFeature

class FunctionalNormedSum(
  c: Seq[Feature],
  wf: () => Double
) extends NormalizedSum(c)
  with FunctionWeightedFeature {
  weightFn = Some(wf)
}
