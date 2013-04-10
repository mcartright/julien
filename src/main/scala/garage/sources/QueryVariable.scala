package garage
package sources

import scala.collection.mutable.ListBuffer

object QueryVariable {
  def apply(features: List[Feature]) : QueryVariable = {
    val weights = List.fill(features.size)(1.0)
    new QueryVariable(features, weights)
  }

  def apply(features: List[Feature], weights: List[Double]) : QueryVariable =
    new QueryVariable(features, weights)
}

class QueryVariable(
  val features: List[Feature],
  val weights: List[Double]
) {
  val sources = ListBuffer[BoundSource]()

  // Precondition check
  require(weights.size == features.size,
    "|features| != |weights| (${features.size} != ${weights.size})")

  def score: Double = {
    features.zip(weights).foldLeft(0.0) { case(score, (f, w)) =>
        score + (f() * w)
    }
  }
}
