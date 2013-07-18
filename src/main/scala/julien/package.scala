import edu.umass.ciir.macros.Macros._
import annotation.elidable

import language.implicitConversions
import reflect.runtime.universe._

/** Provides classes that are typically used by Julien applications.
  *
  * ==Overview==
  *
  * Julien is a retrieval toolkit built on top of the
  * [[http://http://www.lemurproject.org/galago.php Galago]] search system.
  * Specifically, Julien uses trimmed versions of TupleFlow and the index
  * layer of Galago as data sources to a composable retrieval stack implemented
  * in Julien.
  *
  * The [[julien.access access]] package provides classes and traits that mostly
  * deal with direct access to an underlying index for easier data exploration
  * work in the Scala [[http://www.scala-lang.org/node/2097 REPL]].
  *
  * The [[julien.retrieval retrieval]] package focuses more on creating
  * retrieval models to run over a set collection of documents, stored in an
  * index. Components such as scoring functions and query processors are in
  * this package.
  *
  * The [[julien.eval eval]] package is a port of the Galago eval package,
  * with minor modifications to more tightly integrate the evaluation
  * components with the other parts of Julien (for example, all processors
  * return a [[julien.eval.QueryResult QueryResult]], which can be directly
  * passed to an evaluation function such as
  * [[julien.eval.MeanAveragePrecision MAP]] along with a
  * [[julien.eval.QueryJudgments QueryJudgments]] to produce a score).
  *
  * The [[julien.flow flow]] package is a higher-level wrapper around TupleFlow
  * to facilitate constructing TupleFlow jobs. Given the conciseness of Scala
  * and several highly-touted packages that facilitate distributed computation
  * (i.e. [[http://akka.io/ akka]]), we may eventually replace much of the
  * communication and mangement logic of TupleFlow - however the type system
  * will most likely stay, as that is the core idea of the system, and makes it
  * fairly flexible, once jobs are built.
  *
  */
package object julien {
  /**
    * Value for a document identifier. We "wrap" these with a value class
    * because even though the underlying type is an Int, we don't want to
    * treat it as a numeric (i.e. multiplying two docs is meaningless).
    *
    * Good use of a value class - when you want an AnyVal, but you want to
    * actually *remove* some of the functionality of the AnyVal.
    *
    * See SIP-15 (http://docs.scala-lang.org/sips/pending/value-classes.html)
    * for value class details.
    *
    * Also see SIP-13 for details on implicit classes
    * (http://docs.scala-lang.org/sips/pending/implicit-classes.html)
    *
    */
  implicit class InternalId(val underlying: Int) extends AnyVal
  implicit object InternalIdOrder extends Ordering[InternalId] {
    def compare(a: InternalId, b: InternalId) = a.underlying compare b.underlying
  }
  implicit def InternalId2int(d: InternalId): Int = d.underlying
  implicit def InternalId2str(d: InternalId): String = d.underlying.toString

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
  //implicit def regexToRichRegex(r: Regex) = new RichRegex(r)

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
  def printTime[R](label:String)(block: => R): R = {
    val t0 = System.currentTimeMillis
    val result = block
    val t1 = System.currentTimeMillis
    debugf("%s: %d ms\n", label, (t1-t0).toInt)  // this is a test macro
    result
  }

  def time[R](block: => R): Tuple2[R, Long] = {
    val t0 = System.nanoTime
    val result = block
    val t1 = System.nanoTime
    (result, t1-t0)
  }

  // Returns the concrete reflective type of any instantiated object.
  def getType[T](obj: T)(implicit tag: TypeTag[T]): Type = tag.tpe

  /** For debugging. This one is elidable, meaning given the correct flag,
    * the compiler will remove the call and the bytecode for this function.
    */
  val debugPatterns =
    System.getProperty("julien.debug", ".*").split(",").map(_.r)

  @elidable(elidable.FINEST) def debug(msg: String, tags: String*): Unit = {
    val result = debugPatterns.forall(dp => tags.exists(t => dp matches t))
    if (result) Console.err.println(tags.mkString("(",",",")") + ": " + msg)
  }

  @elidable(elidable.INFO) def info(msg: String) = Console.err.println(msg)
}
