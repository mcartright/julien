package julien

/** A belief assigned by a Feature.
  * Underlying class is Double.
  */
implicit class Score(val underlying: Double) extends AnyVal  {
  def *(l: Long): Score = new Score(underlying * l)
  def +(l: Long): Score = new Score(underlying + l)
  def *(s: Score): Score = new Score(underlying * s.underlying)
  def +(s: Score): Score = new Score(underlying + s.underlying)
  def /(s: Score): Score = new Score(underlying / s.underlying)
  def /(l: Length): Score = new Score(underlying / l.underlying)
}
