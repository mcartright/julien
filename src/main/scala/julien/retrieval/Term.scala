package julien
package retrieval

import julien.galago.core.index.ExtentIterator
import julien.galago.core.util.ExtentArray

object Term {
  def apply(s: String) = new Term(s, None)
  def apply(s: String, f: String) = new Term(s, Some(f))
}

/** Represents a direct connection to an index via a key specified
  * the t parameter. Each Term object carries references to the
  * underlying iterator the Term represents, as well as a reference
  * to the index that generated the iterator.
  *
  * These are always 1-to-1.
  */
final class Term private (val t: String, val field: Option[String])
    extends SparseIterator[ExtentIterator]
    with PositionStatsView {

  override def toString: String =
    s"$t: " + (if (isAttached) index.toString else "")

  /** Definition of how this class retrieves its underlying
    * iterator from a given [[Index]] instance.
    */
  def getIterator(i: Index): ExtentIterator = i.shareableIterator(t, field)

  /** Returns the current count of the underlying iterator. */
  def count: Int = if (matched) underlying.count else 0

  /** Returns the current positions of the underlying iterator. */
  def positions: ExtentArray = if (matched) underlying.extents() else ExtentArray.empty

  lazy val statistics: CountStatistics = {
    val ns = underlying.asInstanceOf[ARNA].getStatistics
    val cs = attachedIndex.
      lengthsIterator(field).
      asInstanceOf[ARCA].
      getStatistics

    CountStatistics(
      ns.nodeFrequency,
      cs.documentCount,
      cs.collectionLength,
      ns.nodeDocumentCount,
      ns.maximumCount.toInt,
      attachedIndex.collectionStats.maxLength.toInt
    )
  }
}
