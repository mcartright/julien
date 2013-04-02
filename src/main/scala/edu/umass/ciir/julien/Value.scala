package edu.umass.ciir.julien

/** Base trait for any value we might throw around the in the system.
  * Might be a little overkill, but it allows for very strong type-checking
  * at compile time.
  *
  */
trait Value extends Any

/**
  * Value for a document identifier.
  * Underlying class is an Int.
  */
case class Docid(val underlying: Int) extends AnyVal with Value

/** Value for the maximum count of a particular count op.
  */
case class MaximumCount(val underlying: Int) extends AnyVal with Value

/** The length of any retrievable item.
  * Underlying value is Int.
  */
class Length(val underlying: Int) extends AnyVal with Value {
  def +(i: Int): Int = underlying + i
  def +(l: Long): Long = underlying + l
  def +(d: Double): Double = underlying + d
  def *(i: Int): Int = underlying * i
  def *(l: Long): Long = underlying * l
  def *(d: Double): Double = underlying * d
}

/** The number of times a key (term) occurs in a particular target (doc).
  * Underlying class is Int.
  */
class Count(val underlying: Int) extends AnyVal with Value {
  def +(i: Int): Int = underlying + i
  def +(l: Long): Long = underlying + l
  def +(d: Double): Double = underlying + d
  def *(i: Int): Int = underlying * i
  def *(l: Long): Long = underlying * l
  def *(d: Double): Double = underlying * d
}

/** The count of how many times a key (term) occurs in the
  * universe (collection).
  * Underlying class is Long.
  */
class CollFreq(val underlying: Long) extends AnyVal with Value {
  def +(i: Int): Long = underlying + i
  def +(l: Long): Long = underlying + l
  def +(d: Double): Double = underlying + d
  def *(i: Int): Long = underlying * i
  def *(l: Long): Long = underlying * l
  def *(d: Double): Double = underlying * d
}

/** The number of targets (docs) a particular key (term) occurs in.
  * Underlying class is Int.
  */
case class DocFreq(val underlying: Long) extends AnyVal with Value


/** A belief assigned by a Feature.
  * Underlying class is Double.
  */
class Score(val underlying: Double) extends AnyVal with Value {
  def *(l: Long): Score = new Score(underlying * l)
  def +(l: Long): Score = new Score(underlying + l)
  def *(s: Score): Score = new Score(underlying * s.underlying)
  def +(s: Score): Score = new Score(underlying + s.underlying)
  def /(s: Score): Score = new Score(underlying / s.underlying)
  def /(l: Length): Score = new Score(underlying / l.underlying)
}

/** The size of the collection.
  * Underlying class is Long.
  */
class CollLength(val underlying: Long) extends AnyVal with Value {
  def toDouble: Double = underlying.toDouble
}

/** Number of targets (documents) in the universe (collection).
  * Underlying class is Long.
  */
case class NumDocs(val underlying: Long) extends AnyVal with Value

/** Number of keys (terms) in the universe (collection).
  * Underlying class is Long.
  */
case class VocabSize(val underlying: Long) extends AnyVal with Value

