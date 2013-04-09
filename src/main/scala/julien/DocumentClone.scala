package julien

object DocumentClone {
  def apply(original: Document) =
    new DocumentClone(
      original.length,
      original.content,
      original.vocabulary,
      original.histogram,
      original.multinomial)
}

class DocumentClone(
  val length: Length,
  val content: String,
  val vocabulary: Set[String],
  val histogram: Map[String, Int],
  val multinomial: Map[String, Double]
) extends Document {
  def copy: DocumentClone = DocumentClone(this)
}
