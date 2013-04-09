package julien
package access

import scala.collection.LinearSeq
import org.lemurproject.galago.core.index.DataIterator
import org.lemurproject.galago.core.index.corpus.CorpusReader
import org.lemurproject.galago.core.index.corpus.CorpusReader._
import scala.collection.JavaConversions._
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.{Builder,ListBuffer}
import edu.umass.ciir.julien.IndexBasedDocument._

object DocumentSeq {
  def apply(idx: Index): DocumentSeq = new DocumentSeq(idx)

  implicit def canBuildFrom:
      CanBuildFrom[DocumentSeq, Document, List[Document]] =
    new CanBuildFrom[DocumentSeq, Document, List[Document]] {
      def apply(): Builder[Document, List[Document]] = newBuilder
      def apply(from: DocumentSeq): Builder[Document, List[Document]] =
        newBuilder
    }
  // Overriding to provide cloning of current doc view.
  def newBuilder: Builder[Document, List[Document]] =
    new Builder[Document, List[Document]] {
      val buf = new ListBuffer[Document]
      def result: List[Document] = buf.toList
      def clear() = buf.clear()
      def +=(elem: Document) = {
        buf += elem.copy
        this
      }
    }
}

class DocumentSeq(index: Index)
  (implicit factory: (DataIterator[GDoc], Index) => Document)
    extends LinearSeq[Document] {

  val underlying: DataIterator[GDoc] = index.
    underlying.
    getIndexPart("corpus").
    asInstanceOf[CorpusReader].
    getIterator("dummy").
    asInstanceOf[DataIterator[GDoc]]

  def length: Int = underlying.totalEntries.toInt

  def apply(idx: Int): Document = {
    underlying.syncTo(idx)
    factory(underlying, index)
  }

  override def iterator: Iterator[Document] = new Iterator[Document] {
    // Use another iterator for correctness safety
    val other = index.
      underlying.
      getIndexPart("corpus").
      asInstanceOf[CorpusReader].
      getIterator("dummy").
      asInstanceOf[DataIterator[GDoc]]

    def hasNext = !other.isDone
    def next: Document = {
      val prev: Document = factory(other, index)
      other.movePast(other.currentCandidate)
      prev
    }
  }
}
