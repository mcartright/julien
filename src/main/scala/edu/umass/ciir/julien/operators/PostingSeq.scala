package operators

import org.lemurproject.galago.core.index.ExtentIterator

/** Encapsulation of a single posting in the index.
  * An Index object can produce SeqViews over these
  * given a ViewOp.
  */
trait Posting { def docid: Docid }
trait CountPosting extends Posting with CountSrc
trait PositionsPosting extends CountPosting with PositionSrc
trait ScorePosting extends Posting with ScoreSrc
trait DataPosting extends Posting with DataSrc

class PostingSeq private[operators] (underlying: ExtentIterator)
    extends LinearSeq[+T <: Posting]
    with LinearSeqLike[T, PostingSeq] {
  private[this] val posting: T = T()
  def length: Int = underlying.totalEntries.toInt
  def apply(idx: int): T
  def iterator: Iterator[T]
  def isEmpty: Boolean
  def head: T =
  def tail: PostingSeq = {
    underlying.movePast(underlying.currentCandidate)
    this
  }
}


