package julien

/** A view of a single Document in the collection.
  * An [[Index]] can support a [[julien.seq.DocumentSeq]]
  * over its collection for instances that implement this
  * behavior.
  */
trait Document extends LengthsSrc {
  def identifier: Docid
  def name: String
  def length: Length
  def content: String
  def termVector: List[String]
  def vocabulary: Set[String]
  def copy: Document

  def histogram: Map[String, Int] =
    termVector.groupBy { case s => s }.mapValues(_.size)
  def multinomial: Map[String, Double] = {
    val h = termVector.groupBy { case s => s }.mapValues(_.size)
    val sum = h.values.sum
    h.mapValues(_.toDouble / sum)
  }

}

object Document {
  def apply(gd: GDoc): Document = DocumentClone(gd)
}
