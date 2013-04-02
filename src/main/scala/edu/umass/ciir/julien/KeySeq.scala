package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.KeyIterator
import org.lemurproject.galago.tupleflow.Utility
import scala.collection.{SeqView, SeqViewLike}

class KeySeq private[operators](iterGen: => KeyIterator)
    extends SeqView[String, KeyIterator]
    with SeqViewLike[String, KeyIterator, KeySeq] {
  override val underlying = iterGen
  def length: Int = -1
  def apply(idx: Int): String = {
    for(i <- 0 until idx) if (!underlying.isDone) underlying.nextKey
    Utility.toString(underlying.getKey)
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
