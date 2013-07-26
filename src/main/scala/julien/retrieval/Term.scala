package julien
package retrieval

import scala.concurrent.{Future,Await,future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import julien.galago.core.index.ExtentIterator
import julien.galago.core.util.ExtentArray

object Term {
  def apply(s: String)(implicit i: Index) = new StreamedTerm(s, None, None, i)
  def apply(s: String, f: String)(implicit i: Index) =
    new StreamedTerm(s, Some(f), None, i)
  def apply(s: String, f: String, stem: String)(implicit i: Index) =
    new StreamedTerm(s, Some(f), Some(stem), i)
}

sealed abstract class Term(t: String, index: Index)
  extends PositionStatsView
  with PositionsBufferView
  with ChildlessOp {
  override def toString: String = s"$t: " + index.toString
  override def terse: String = s"$t"
}

/** Represents a direct connection to an index via a key specified
  * the t parameter. Each Term object carries references to the
  * underlying iterator the Term represents, as well as a reference
  * to the index that generated the iterator.
  *
  * These are always 1-to-1.
  */
final class StreamedTerm (
  val t: String,
  val field: Option[String],
  val stem: Option[String],
  override val index: Index
)
    extends Term(t, index)
    with SparseIterator[ExtentIterator] {

  override lazy val underlying = index.shareableIterator(
    t,
    field.getOrElse(index.defaultField),
    stem.getOrElse(index.defaultStem)
  )

  /** Returns the current count of the underlying iterator. */
  def count(id: InternalId): Int = {
    underlying.syncTo(id)
    if (underlying.hasMatch(id)) underlying.count else 0
  }

  /** Returns the current positions of the underlying iterator. */
  def positions(id: InternalId): ExtentArray = {
    underlying.syncTo(id)
    if (underlying.hasMatch(id)) underlying.extents() else ExtentArray.empty
  }

  /** Return the underlying buffer of extents. Note that the buffer will be
    * volatile (it will be directly updated by the iterator underlying this
    * view.
    */
  def positionsBuffer: ExtentArray = underlying.extents()

  lazy val statistics: CountStatistics = {
    val ns = underlying.asInstanceOf[ARNA].getStatistics
    val cs = index.
      lengthsIterator(field).
      asInstanceOf[ARCA].
      getStatistics

    CountStatistics(
      ns.nodeFrequency,
      cs.documentCount,
      cs.collectionLength,
      ns.nodeDocumentCount,
      ns.maximumCount.toInt,
      index.collectionStats.maxLength.toInt
    )
  }

  case class Posting(var docid: Int, var positions: ExtentArray)
  def walker : Traversable[Posting] = new Traversable[Posting] {
    val thePosting = Posting(0, ExtentArray.empty)
    def foreach[U](f: Posting => U) {
      val start = at
      reset
      while (!isDone) {
        thePosting.docid = at
        thePosting.positions = underlying.extents
        f(thePosting)
        movePast(at)
      }
      reset
      moveTo(start)
    }
  }
}
