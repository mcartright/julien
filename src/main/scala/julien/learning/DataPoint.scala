package julien
package learning

object DataPoint {
  val UNKNOWN = -1000000F
  val MAX_FEATURE = 51
  val FEATURE_INCREASE = 10
}

class DataPoint(
  val id: String,
  val label: Float,
  val description: String,
  val features: List[Double]
)
