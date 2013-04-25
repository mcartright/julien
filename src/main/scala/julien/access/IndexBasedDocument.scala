package julien
package access

import org.lemurproject.galago.core.index.DataIterator
import scala.collection.JavaConversions._

class IndexBasedDocument extends Document {
  var index: Index = null
  var underlying: GDoc = null
  def identifier: Docid = Docid(underlying.identifier)
  def name: String = underlying.name
  def length = index.length(underlying.name)
  def content: String = underlying.text

  // These depend on the term vector being present
  def hasTermVector: Boolean = (underlying.terms != null)
  def termVector: List[String] = underlying.terms.toList
  def vocabulary: Set[String] = underlying.terms.toSet
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
