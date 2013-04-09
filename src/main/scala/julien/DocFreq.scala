package julien

/** The number of targets (docs) a particular key (term) occurs in.
  * Underlying class is Int.
  */
implicit class DocFreq(val underlying: Long) extends AnyVal {
  def +(d: Double): Double = d + underlying
  def +(l: Long): DocFreq = DocFreq(underlying + l)
  def +(i: Int): DocFreq = DocFreq(underlying + i)
}
