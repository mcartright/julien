package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.ExtentIterator
import scala.collection.{SeqView, SeqViewLike}
import org.lemurproject.galago.tupleflow.Utility

/** Encapsulation of a single posting in the index.
  * An Index object can produce SeqViews over these
  * given a ViewOp.
  */
trait Posting { def docid: Docid }
class CountPosting extends Posting with CountSrc {
  var d: Docid = new Docid(0)
  def docid: Docid = d
  var c: Count = new Count(0)
  def count: Count = c
}

object CountPosting {
  val thePosting = new CountPosting
  implicit def apply(e: ExtentIterator) = {
    thePosting.d = new Docid(e.currentCandidate)
    thePosting.c = new Count(e.count)
    thePosting
  }
}

class PositionsPosting extends CountPosting with PositionSrc {
  var p: Positions = Positions()
  def positions: Positions = p
}

object PositionsPosting {
  val thePosting = new PositionsPosting
  implicit def apply(e: ExtentIterator) = {
    thePosting.d = new Docid(e.currentCandidate)
    thePosting.c = new Count(e.count)
    thePosting.p = Positions(e.extents)
    thePosting
  }
}

// trait ScorePosting extends Posting with ScoreSrc
// trait DataPosting extends Posting with DataSrc

class PostingSeq[T <: Posting] (
  override val underlying: ExtentIterator,
  index: Index
)(implicit factory: (ExtentIterator) => T)
    extends SeqView[T, ExtentIterator]
    with SeqViewLike[T, ExtentIterator, PostingSeq[T]] {
  def length: Int = underlying.totalEntries.toInt
  def apply(idx: Int): T = {
    underlying.syncTo(idx)
    factory(underlying)
  }

  override def isEmpty: Boolean = underlying.totalEntries == 0L
  override def head: T = factory(underlying)
  override def tail: PostingSeq[T] = {
    underlying.movePast(underlying.currentCandidate)
    this
  }

  override def iterator: Iterator[T] = new Iterator[T] {
    val other = index.iterator(Utility.toString(underlying.key))
    def hasNext = !other.isDone
    def next: T = {
      val prev: T = factory(other)
      other.movePast(other.currentCandidate)
      prev
    }
  }
}
