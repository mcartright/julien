package julien

/** Number of targets (documents) in the universe (collection).
  * Underlying class is Long.
  */
implicit class NumDocs(val underlying: Long) extends AnyVal {
  def /(d: Double): Double = underlying.toDouble / d
}
