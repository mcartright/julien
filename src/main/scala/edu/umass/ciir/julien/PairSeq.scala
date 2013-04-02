package edu.umass.ciir.julien

import scala.collection.{SeqView, SeqViewLike}
import org.lemurproject.galago.core.index.KeyIterator
import org.lemurproject.galago.tupleflow.Utility

class PairSeq[V] (
  iterGen: => KeyIterator,
  valueGen: KeyIterator => V)
    extends SeqView[Tuple2[String, V], KeyIterator]
    with SeqViewLike[Tuple2[String, V], KeyIterator, PairSeq[V]] {
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

  def iterator: Iterator[Tuple2[String, V]] = new Iterator[Tuple2[String, V]] {
    val other = iterGen
    def hasNext = !other.isDone
    def next: Tuple2[String, V] = {
      val v = (Utility.toString(other.getKey), valueGen(other))
      other.nextKey
      v
    }
  }
}
