package sources

trait Feature {
  def apply(): Double = calculate
  def calculate: Double
  def upperBound: Double = Double.PositiveInfinity
  def lowerBound: Double = Double.NegativeInfinity
}
