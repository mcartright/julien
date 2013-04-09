package julien

/** The length of any retrievable item.
  * Underlying value is Int.
  */
implicit class Length(val underlying: Int) extends AnyVal  {
  def +(i: Int): Int = underlying + i
  def +(l: Long): Long = underlying + l
  def +(d: Double): Double = underlying + d
  def *(i: Int): Int = underlying * i
  def *(l: Long): Long = underlying * l
  def *(d: Double): Double = underlying * d
}
