package julien

import scala.collection.JavaConversions._

object DocumentClone {
  def apply(gd: GDoc): Document =
    new DocumentClone(
      Docid(gd.identifier),
      gd.name,
      new Length(gd.terms.size),
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

class DocumentClone(
  val identifier: Docid,
  val name: String,
  val length: Length,
  val content: String,
  val termVector: List[String],
  val vocabulary: Set[String]
) extends Document {
  def copy: DocumentClone = DocumentClone(this)
}
