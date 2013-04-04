package edu.umass.ciir.julien



/** A view of a single Document in the collection.
  * An Index can support an Iterator over the collection
  * for this kind of object.
  */
trait Document extends LengthsSrc {
  def length: Length
  def count(op: CountView): Count
  def positions(op: PositionsView): Positions
  def content: String
  def vocabulary: Set[String]
  def histogram: Map[String, Int]
  def multinomial: Map[String, Double]
}

