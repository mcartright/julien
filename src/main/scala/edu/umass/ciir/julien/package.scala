package edu.umass.ciir

/** Correctness enforced using value classes for any value we might throw
  * around the in the system. Might be a little overkill, but it allows
  * for strong type-checking at compile time.
  *
  *  See SIP-15 (http://docs.scala-lang.org/sips/pending/value-classes.html)
  *  for value class details.
  */

package object julien {
  /**
    * Value for a document identifier.
    * Underlying class is an Int.
    */
  implicit class Docid(val underlying: Int) extends AnyVal

  /** Value for the maximum count of a particular count op.
    */
  implicit class MaximumCount(val underlying: Int) extends AnyVal

  /** The length of any retrievable item.
    * Underlying value is Int.
    */
  class Length(val underlying: Int) extends AnyVal  {
    def +(i: Int): Int = underlying + i
    def +(l: Long): Long = underlying + l
    def +(d: Double): Double = underlying + d
    def *(i: Int): Int = underlying * i
    def *(l: Long): Long = underlying * l
    def *(d: Double): Double = underlying * d
  }
  implicit def len2int(l: Length): Int = l.underlying

  /** The number of times a key (term) occurs in a particular target (doc).
    * Underlying class is Int.
    */
  class Count(val underlying: Int) extends AnyVal  {
    def +(i: Int): Int = underlying + i
    def +(l: Long): Long = underlying + l
    def +(d: Double): Double = underlying + d
    def *(i: Int): Int = underlying * i
    def *(l: Long): Long = underlying * l
    def *(d: Double): Double = underlying * d
    def /(l: Length): Double = underlying.toDouble / l.underlying
  }

  /** The count of how many times a key (term) occurs in the
    * universe (collection).
    * Underlying class is Long.
    */
  class CollFreq(val underlying: Long) extends AnyVal  {
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
  implicit class DocFreq(val underlying: Long) extends AnyVal {
    def +(d: Double): Double = d + underlying
  }


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

  /** The size of the collection.
    * Underlying class is Long.
    */
  implicit class CollLength(val underlying: Long) extends AnyVal {
    def /(nd: NumDocs): Double = underlying.toDouble / nd.underlying
  }
  implicit def cl2long(cl: CollLength) = cl.underlying

  /** Number of targets (documents) in the universe (collection).
    * Underlying class is Long.
    */
  implicit class NumDocs(val underlying: Long) extends AnyVal {
    def /(d: Double): Double = underlying.toDouble / d
  }

  /** Number of keys (terms) in the universe (collection).
    * Underlying class is Long.
    */
  implicit class VocabSize(val underlying: Long) extends AnyVal
}
