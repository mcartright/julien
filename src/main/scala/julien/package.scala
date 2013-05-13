import edu.umass.ciir.macros.Macros._
import annotation.elidable

import language.implicitConversions

/** Provides classes that are typically used by Julien applications.
  *  ==Overview==
  *
  */
package object julien {
/** Correctness enforced using value classes for any value we might throw
  *  around the in the system. Might be a little overkill, but it allows
  *  for strong type-checking at compile time.
  *
  *   See SIP-15 (http://docs.scala-lang.org/sips/pending/value-classes.html)
  *   for value class details.
  *
  *  Also using SIP-13, implicit classes
  *  (http://docs.scala-lang.org/sips/pending/implicit-classes.html)
  */

  /**
    * Value for a document identifier. We "wrap" these with a value class
    * because even though the underlying type is an Int, we don't want to
    * treat it as a numeric (i.e. multiplying two docs is meaningless).
    *
    * Good use of a value class - when you want an AnyVal, but you want to
    * actually *remove* some of the functionality of the AnyVal.
    */
  implicit class InternalId(val underlying: Int) extends AnyVal
  implicit object InternalIdOrder extends Ordering[InternalId] {
    def compare(a: InternalId, b: InternalId) = a.underlying compare b.underlying
  }
  implicit def InternalId2int(d: InternalId): Int = d.underlying

  import scala.util.matching.Regex
  /** Implicit extension to the Regex class (done via composition)
  * this class provides easy "match" and "miss" type methods against
  * regular Strings. Only restriction is that the RichRegex must come
  * first in the comparison. See examples in the methods below.
  */
  implicit class RichRegex(underlying: Regex) {
    // TODO: Add some memoization of the underlying patterns. Should be done
    //       in the object, I imagine, as they should be shared once compiled.

    /** Returns true if the given string matches this pattern. Example:
      *
      * """\d+""".r matches "90210"  => True
      */
    def matches(s: String): Boolean = underlying.pattern.matcher(s).matches

    /** @see #matches(s: String)
      */
    def ==(s: String): Boolean = matches(s)

    /** Logical negation of #matches(s: String).
      */
    def misses(s: String): Boolean = (matches(s) == false)

    /** @see #misses(s: String)
      */
    def !=(s: String): Boolean = misses(s)
  }

  /** Implicit lift of Regex to RichRegex. */
  implicit def regexToRichRegex(r: Regex) = new RichRegex(r)

  // Explicitly for the implicit below (get it?)
  import scala.collection.mutable.{ListBuffer,PriorityQueue}
  implicit def q2list[T](q: PriorityQueue[T]): List[T] = {
    val b = ListBuffer[T]()
    while (!q.isEmpty) { b += q.dequeue }
    b.toList
  }

  /** Type definitions, most of which are for aliasing in the
    * package.
    */

  // To bring the packages in scope...
  import julien.galago.core.index._
  import julien.galago.core.index.corpus._

  // Because the names are ridiculously long...
  type GIndex = julien.galago.core.index.Index
  type GDoc = julien.galago.core.parse.Document
  type GIterator = julien.galago.core.index.Iterator
  type ARCA = AggregateReader.CollectionAggregateIterator
  type ARNA = AggregateReader.NodeAggregateIterator
  type NS = AggregateReader.NodeStatistics
  type CS = AggregateReader.CollectionStatistics
  type TEI = ExtentIterator
  type TCI = CountIterator
  type LI = LengthsReader.LengthsIterator

  /** For debugging/timing purposes, until I can figure out a macro to
    * compile this out - At least moving the definition will be easy.
    */
  def time[R](label:String)(block: => R): R = {
    val t0 = System.currentTimeMillis
    val result = block
    val t1 = System.currentTimeMillis
    debugf("%s: %d ms\n", label, (t1-t0).toInt)  // this is a test macro
    result
  }

  /** For debugging. This one is elidable, meaning given the correct flag,
    * the compiler will remove the call and the bytecode for this function.
    */
  @elidable(elidable.FINEST) def debug(msg: String) = Console.err.println(msg)
}
