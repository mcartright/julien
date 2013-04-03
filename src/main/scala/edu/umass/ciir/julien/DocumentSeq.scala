package edu.umass.ciir.julien

import scala.collection.{SeqView, SeqViewLike}
import edu.umass.ciir.julien.Aliases._
import org.lemurproject.galago.core.index.DataIterator
import org.lemurproject.galago.core.index.corpus.CorpusReader
import org.lemurproject.galago.core.index.corpus.CorpusReader._
import scala.collection.JavaConversions._

/** A view of a single Document in the collection.
  * An Index can support an Iterator over the collection
  * for this kind of object.
  */
trait Document extends LengthsSrc {
  def length: Length
  def count(op: CountOp): Count
  def positions(op: PositionsOp): Positions
  def content: String
  def vocabulary: Set[String]
  def histogram: Map[String, Int]
  def multinomial: Map[String, Double]
}

class IndexBasedDocument extends Document {
  var index: Index = null
  var underlying: GDoc = null
  def length = new Length(index.length(underlying.name))
  def count(op: CountOp) = op.count
  def positions(op: PositionsOp) = op.positions
  def content: String = underlying.text

  // These depend on the term vector being present
  def vocabulary: Set[String] = underlying.terms.toSet
  def histogram: Map[String, Int] =
    underlying.terms.groupBy { case s => s }.mapValues(_.size)
  def multinomial: Map[String, Double] = {
    val h = underlying.terms.groupBy { case s => s }.mapValues(_.size)
    val sum = h.values.sum
    h.mapValues(_.toDouble / sum)
  }
}

object IndexBasedDocument {
  val theDocument = new IndexBasedDocument
  implicit def apply(ci: DataIterator[GDoc], idx: Index): IndexBasedDocument = {
    theDocument.index = idx
    theDocument.underlying = ci.getData
    theDocument
  }

  def apply(d: GDoc, idx: Index): IndexBasedDocument = {
    val doc = new IndexBasedDocument
    doc.index = idx
    doc.underlying = d
    doc
  }
}

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
