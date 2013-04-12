package julien
package retrieval

import org.lemurproject.galago.core.index.ExtentIterator

object Term { def apply(s: String) = new Term(s) }

/** Represents a direct connection to an index via a key specified
  * the t parameter. Each Term object carries references to the
  * underlying iterator the Term represents, as well as a reference
  * to the index that generated the iterator.
  *
  * These are always 1-to-1.
  */
final class Term(val t: String)
    extends IteratedHook[ExtentIterator]
    with CountSrc
    with PositionSrc {

  override def toString: String =
    s"$t:" + (if (isAttached) index.toString else "")

  /** Definition of how this class retrieves its underlying
    * iterator from a given [[Index]] instance.
    */
  def getIterator(i: Index): ExtentIterator = i.shareableIterator(t)

  /** Returns the current count of the underlying iterator. */
  def count: Count = new Count(underlying.count)

  /** Returns the current positions of the underlying iterator. */
  def positions: Positions = Positions(underlying.extents())

  lazy val statistics: CountStatistics = {
    val ns = attachedIndex.asInstanceOf[ARNA].getStatistics
    val cs = attachedIndex.lengthsIterator.asInstanceOf[ARCA].getStatistics
    CountStatistics(
      new CollFreq(ns.nodeFrequency),
      new NumDocs(cs.documentCount),
      new CollLength(cs.collectionLength),
      new DocFreq(ns.nodeDocumentCount),
      new MaximumCount(ns.maximumCount.toInt)
    )
  }
}
