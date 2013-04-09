import edu.umass.ciir.macros.Macros._

/** Provides classes that are typically used by Julien applications.
  *  ==Overview==
  *
  */
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


  implicit def len2int(l: Length): Int = l.underlying


  implicit def cl2long(cl: CollLength) = cl.underlying


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
    */
  type Combiner = (Seq[FeatureOp]) => Score

  // To bring the packages in scope...
  import org.lemurproject.galago.core.index._
  import org.lemurproject.galago.core.index.corpus._

  // Because the names are ridiculously long...
  type GIndex = org.lemurproject.galago.core.index.Index
  type GDoc = org.lemurproject.galago.core.parse.Document
  type GIterator = org.lemurproject.galago.core.index.Iterator
  type ARCA = AggregateReader.CollectionAggregateIterator
  type ARNA = AggregateReader.NodeAggregateIterator
  type NS = AggregateReader.NodeStatistics
  type CS = AggregateReader.CollectionStatistics
  type TEI = ExtentIterator
  type TCI = CountIterator
  type MLI = LengthsReader.LengthsIterator

  // For debugging/timing purposes, until I can figure out a macro to
  // compile this out - At least moving the definition will be easy.
  def time[R](label:String)(block: => R): R = {
    val t0 = System.currentTimeMillis
    val result = block
    val t1 = System.currentTimeMillis
    debugf("%s: %d ms\n", label, (t1-t0).toInt)  // this is a test macro
    result
  }

  /** Implicit lift of Regex to RichRegex. */
  implicit def regexToRichRegex(r: Regex) = new RichRegex(r)
}
