package julien
package retrieval

import julien.galago.core.index.ExtentIterator
import julien.galago.core.util.ExtentArray

object Term {
  def apply(s: String)(implicit i: Index) = new Term(s, None, None, i)
  def apply(s: String, f: String)(implicit i: Index) =
    new Term(s, Some(f), None, i)
  def apply(s: String, f: String, stem: String)(implicit i: Index) =
    new Term(s, Some(f), Some(stem), i)
}

/** Represents a direct connection to an index via a key specified
  * the t parameter. Each Term object carries references to the
  * underlying iterator the Term represents, as well as a reference
  * to the index that generated the iterator.
  *
  * These are always 1-to-1.
  */
final class Term private (
  val t: String,
  val field: Option[String],
  val stem: Option[String],
  override val index: Index
)
    extends SparseIterator[ExtentIterator]
    with PositionStatsView {

  override def toString: String = s"$t: " + index.toString

  override val underlying = index.shareableIterator(
    t,
    field.getOrElse(index.defaultField),
    stem.getOrElse(index.defaultStem)
  )

  /** Returns the current count of the underlying iterator. */
  def count: Int = if (matched) underlying.count else 0

  /** Returns the current positions of the underlying iterator. */
  def positions: ExtentArray =
    if (matched) underlying.extents() else ExtentArray.empty

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
        thePosting.positions = positions
        f(thePosting)
        movePast(at)
      }
      reset
      moveTo(start)
    }
  }
}
