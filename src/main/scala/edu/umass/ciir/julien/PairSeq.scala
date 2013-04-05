package edu.umass.ciir.julien

import scala.collection.LinearSeq
import org.lemurproject.galago.core.index.KeyIterator
import org.lemurproject.galago.tupleflow.Utility

// We only inherit from LinearSeq here, because LinearSeqLike indicates
// that the PairSeq class should be the result of a transformation
// (e.g. tail, or map). LinearSeq only expects a generic LinearSeq,
// which is what we want.
class PairSeq[V] (
  iterGen: => KeyIterator,
  valueGen: KeyIterator => V)
    extends LinearSeq[Tuple2[String, V]] {
  val underlying = iterGen
  def apply(idx: Int): Tuple2[String, V] = {
    underlying.reset
    for (i <- 0 until idx) if (!underlying.isDone) underlying.nextKey
    (Utility.toString(underlying.getKey), valueGen(underlying))
  }

  lazy val length: Int = {
    underlying.reset
    var count = 0
    while (!underlying.isDone) { count += 1; underlying.nextKey }
    count
  }

  override def head: Tuple2[String, V] =
    (Utility.toString(underlying.getKey), valueGen(underlying))

  override def tail: PairSeq[V] = {
    underlying.nextKey
    this
  }

  override def isEmpty: Boolean = {
    underlying.reset
    underlying.isDone
  }

  // faster override of the iterator
  override def iterator: Iterator[Tuple2[String, V]] =
    new Iterator[Tuple2[String, V]] {
      val other = iterGen
      def hasNext = !other.isDone
      def next: Tuple2[String, V] = {
        val v = (Utility.toString(other.getKey), valueGen(other))
        other.nextKey
        v
      }
    }
}
