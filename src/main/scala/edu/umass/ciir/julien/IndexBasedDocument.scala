package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.DataIterator
import scala.collection.JavaConversions._

class IndexBasedDocument extends Document {
  var index: Index = null
  var underlying: GDoc = null
  def length = new Length(index.length(underlying.name))
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
  def copy: Document = DocumentClone(this)
}

object IndexBasedDocument {
  val theDocument = new IndexBasedDocument
  def apply(ci: DataIterator[GDoc], idx: Index): Document = fromIter(ci, idx)

  implicit def fromIter(ci: DataIterator[GDoc], idx: Index): Document = {
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
