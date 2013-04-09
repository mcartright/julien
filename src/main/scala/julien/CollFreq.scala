package julien

/** The count of how many times a key (term) occurs in the
  * universe (collection).
  * Underlying class is Long.
  */
implicit class CollFreq(val underlying: Long) extends AnyVal  {
  def +(i: Int): Long = underlying + i
  def +(l: Long): Long = underlying + l
  def +(d: Double): Double = underlying + d
  def *(i: Int): Long = underlying * i
  def *(l: Long): Long = underlying * l
  def *(d: Double): Double = underlying * d
}
