package julien

/** A view of a single Document in the collection.
  * An [[Index]] can support a [[julien.seq.DocumentSeq]]
  * over its collection for instances that implement this
  * behavior.
  */
trait Document extends LengthsSrc {
  def length: Length
  def content: String
  def vocabulary: Set[String]
  def histogram: Map[String, Int]
  def multinomial: Map[String, Double]
  def copy: Document
}

