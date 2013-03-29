package edu.umass.ciir.julien

class QueryVariable(
  val features: List[Feature],
  val sources: List[DataSource],
  val weights: List[Double]
) {
  // Precondition check
  require(weights.size == features.size,
    "|features| != |weights| (${features.size} != ${weights.size})")

  def score: Double = {
    features.zip(weights).foldLeft(0.0) { case(score, (f, w)) =>
        score + (f() * w)
    }
  }
}
