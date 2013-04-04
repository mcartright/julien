package edu.umass.ciir.julien

import scala.collection.{SeqView, SeqViewLike}
import org.lemurproject.galago.core.index.DataIterator
import org.lemurproject.galago.core.index.corpus.CorpusReader
import org.lemurproject.galago.core.index.corpus.CorpusReader._
import scala.collection.JavaConversions._

class DocumentSeq[+D <: Document] (index: Index)
  (implicit factory: (DataIterator[GDoc], Index) => D)
    extends SeqView[D, DataIterator[GDoc]]
    with SeqViewLike[D, DataIterator[GDoc], DocumentSeq[D]] {
  import IndexBasedDocument._

  val underlying: DataIterator[GDoc] = index.
    underlying.
    getIndexPart("corpus").
    asInstanceOf[CorpusReader].
    getIterator("dummy").
    asInstanceOf[DataIterator[GDoc]]

  def length: Int = underlying.totalEntries.toInt

  def apply(idx: Int): D = {
    underlying.syncTo(idx)
    factory(underlying, index)
  }

  def iterator: Iterator[D] = new Iterator[D] {
    // Use another iterator for correctness safety
    val other = index.
      underlying.
      getIndexPart("corpus").
      asInstanceOf[CorpusReader].
      getIterator("dummy").
      asInstanceOf[DataIterator[GDoc]]

    def hasNext = !other.isDone
    def next: D = {
      val prev: D = factory(other, index)
      other.movePast(other.currentCandidate)
      prev
    }
  }
}
