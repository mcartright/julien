package operators

/** A view of a single Document in the collection.
  * An Index can support an Iterator over the collection
  * for this kind of object.
  */
trait Document extends Value {
  def length: Length
  def count(op: CountOp): Count
  def positions(op: PositionsOp): Positions
  def content: String
  def vocabulary: Set[String]
  def histogram: Map[String, Int]
  def multinomial: Map[String, Double]
}
