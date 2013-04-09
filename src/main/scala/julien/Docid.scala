package julien


/**
  * Value for a document identifier.
  * Underlying class is an Int.
  */
implicit class Docid(val underlying: Int) extends AnyVal
