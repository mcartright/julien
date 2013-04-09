package julien
package access

import org.lemurproject.galago.core.index.ExtentIterator
import org.lemurproject.galago.tupleflow.Utility
import scala.collection.{LinearSeq}
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.List
import scala.collection.mutable.{Builder,ListBuffer}

object PostingSeq {
  implicit def canBuildFrom[T <: Posting[T]]:
      CanBuildFrom[PostingSeq[T], T, List[T]] =
    new CanBuildFrom[PostingSeq[T], T, List[T]] {
      def apply(): Builder[T, List[T]] = newBuilder
      def apply(from: PostingSeq[T]): Builder[T, List[T]] = newBuilder
    }
  // The builder override here is specifically because we need to copy
  // the "current" posting since it's only a view of the current posting.
  def newBuilder[T <: Posting[T]]: Builder[T, List[T]] =
    new Builder[T, List[T]] {
      val buf = new ListBuffer[T]
      def result: List[T] = buf.toList
      def clear() = buf.clear()
      def +=(elem: T) = {
        buf += elem.copy
        this
      }
    }
}

class PostingSeq[T <: Posting[T]] (
  val underlying: ExtentIterator,
  index: Index)
  (implicit factory: (ExtentIterator) => T)
    extends LinearSeq[T] {
  def length: Int = underlying.totalEntries.toInt
  def apply(idx: Int): T = {
    underlying.syncTo(idx)
    factory(underlying)
  }

  override def newBuilder: Builder[T, List[T]] = PostingSeq.newBuilder
  override def isEmpty: Boolean = underlying.totalEntries == 0L
  override def head: T = factory(underlying)
  override def tail: PostingSeq[T] = {
    underlying.movePast(underlying.currentCandidate)
    this
  }

  // faster override of the iterator
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
