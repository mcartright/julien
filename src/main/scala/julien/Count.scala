package julien

/** The number of times a key (term) occurs in a particular target (doc).
  * Underlying class is Int.
  */
implicit class Count(val underlying: Int) extends AnyVal  {
  def +(i: Int): Int = underlying + i
  def +(l: Long): Long = underlying + l
  def +(d: Double): Double = underlying + d
  def *(i: Int): Int = underlying * i
  def *(l: Long): Long = underlying * l
  def *(d: Double): Double = underlying * d
  def /(l: Length): Double = underlying.toDouble / l.underlying
}
