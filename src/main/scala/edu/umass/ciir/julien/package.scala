package edu.umass.ciir

package object julien {

/** Correctness enforced using value classes for any value we might throw
  * around the in the system. Might be a little overkill, but it allows
  * for strong type-checking at compile time.
  *
  *  See SIP-15 (http://docs.scala-lang.org/sips/pending/value-classes.html)
  *  for value class details.
  *
  * Also using SIP-13, implicit classes
  * (http://docs.scala-lang.org/sips/pending/implicit-classes.html)
  */

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
  implicit class Length(val underlying: Int) extends AnyVal  {
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
  implicit class Count(val underlying: Int) extends AnyVal  {
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
  implicit class CollFreq(val underlying: Long) extends AnyVal  {
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

  // Explicitly for the implicit below (get it?)
  import scala.collection.mutable.{ListBuffer,PriorityQueue}
  implicit def q2list[T](q: PriorityQueue[T]): List[T] = {
    val b = ListBuffer[T]()
    while (!q.isEmpty) { b += q.dequeue }
    b.toList
  }


  // Makes byte-array calls much less annoying
  implicit def string2bytes(s: String) =
    org.lemurproject.galago.tupleflow.Utility.fromString(s)

  /** Type definitions, most of which are for aliasing in the
    * package.
    *
    */
  type Combiner = (Seq[FeatureOp]) => Score

  // To bring the packages in scope...
  import org.lemurproject.galago.core.index._
  import org.lemurproject.galago.core.index.corpus._

  // Because the names are ridiculously long...
  type GIndex = org.lemurproject.galago.core.index.Index
  type GDoc = org.lemurproject.galago.core.parse.Document
  type ARCA = AggregateReader.CollectionAggregateIterator
  type ARNA = AggregateReader.NodeAggregateIterator
  type NS = AggregateReader.NodeStatistics
  type CS = AggregateReader.CollectionStatistics
  type TEI = ExtentIterator
  type TCI = CountIterator
  type MLI = LengthsReader.LengthsIterator
}
