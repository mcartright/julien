package julien

/** The size of the collection.
  * Underlying class is Long.
  */
implicit class CollLength(val underlying: Long) extends AnyVal {
  def /(nd: NumDocs): Double = underlying.toDouble / nd.underlying
}
