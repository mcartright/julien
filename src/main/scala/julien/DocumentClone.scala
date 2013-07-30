package julien

import scala.collection.JavaConversions._

object DocumentClone {
  def apply(gd: GDoc): Document =
    new DocumentClone(
      gd.identifier,
      gd.name,
      gd.terms.size,
      gd.text,
      gd.terms.toList,
      gd.terms.toSet)

  def apply(original: Document) =
    new DocumentClone(
      original.identifier,
      original.name,
      original.length,
      original.content,
      original.termVector,
      original.vocabulary)
}

/** An implementation of the [[julien.Document Document]] interface that does
  * not require an existing connection to an underlying
  * [[julien.access.Index Index]].
  */
class DocumentClone(
  val identifier: Int,
  val name: String,
  val length: Int,
  val content: String,
  val termVector: List[String],
  val vocabulary: Set[String]
) extends Document {
  def copy: DocumentClone = DocumentClone(this)
  def hasTermVector: Boolean = (termVector != null)
}
