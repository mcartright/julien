package julien

import scala.language.existentials

/** A view of a single Document in the collection.
  * An [[Index]] can support a [[julien.seq.DocumentSeq]]
  * over its collection for instances that implement this
  * behavior.
  */
trait Document {
  def identifier: InternalId
  def name: String
  def length: Int
  def content: String
  def hasTermVector: Boolean
  def termVector: List[String]
  def vocabulary: Set[String]
  def copy: Document

  def histogram: Map[String, Int] = {
    assume(hasTermVector, s"Needs term vector available")
    termVector.groupBy { case s => s }.mapValues(_.size)
  }
  def multinomial: Map[String, Double] = {
    assume(hasTermVector, s"Needs term vector available")
    val h = termVector.groupBy { case s => s }.mapValues(_.size)
    val sum = h.values.sum
    h.mapValues(_.toDouble / sum)
  }

}

object Document {
  def apply(gd: GDoc): Document = DocumentClone(gd)

  val empty = new Document {
    val identifier = InternalId(-1)
    val name = "N/A"
    val length = 0
    val content = ""
    val hasTermVector = false
    val termVector = List.empty[String]
    val vocabulary = Set.empty[String]
    val copy = this
    override val histogram = Map.empty[String, Int]
    override val multinomial = Map.empty[String, Double]
  }
}
