package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.KeyIterator
import org.lemurproject.galago.tupleflow.Utility
import scala.collection.{SeqView, SeqViewLike}

class KeySeq(iterGen: => KeyIterator)
    extends SeqView[String, KeyIterator]
    with SeqViewLike[String, KeyIterator, KeySeq] {
  override val underlying = iterGen

  def apply(idx: Int): String = {
    underlying.reset
    for(i <- 0 until idx) if (!underlying.isDone) underlying.nextKey
    Utility.toString(underlying.getKey)
  }

  lazy val length: Int = {
    underlying.reset
    var count = 0
    while (!underlying.isDone) { count += 1; underlying.nextKey }
    count
  }

  def iterator: Iterator[String] = new Iterator[String] {
    val other = iterGen
    def hasNext = !other.isDone
    def next: String = {
      val s: String = Utility.toString(other.getKey)
      other.nextKey
      s
    }
  }
}
