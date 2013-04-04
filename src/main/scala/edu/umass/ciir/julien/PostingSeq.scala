package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.ExtentIterator
import org.lemurproject.galago.tupleflow.Utility
import scala.collection.{SeqView, SeqViewLike}

class PostingSeq[T <: Posting] (
  override val underlying: ExtentIterator,
  index: Index)
  (implicit factory: (ExtentIterator) => T)
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
