package julien
package retrieval

import scala.collection.mutable.{LinearSeq, Builder}

/** This trait defines what an accumulator needs to behave like.
  * Basically, it's some sort of Buffer (we should be able to mutate it),
  * but it's also a sequence of some kind. For now I'm going to call it
  * a LinearSeq (fast head/tail, but not indexed access), because the
  * standard use case is a PriorityQueue (another LinearSeq). This trait
  * also extends Builder, which additionally defines a 'result' method
  * which means you're done adding to the accumulator.
  */
trait Accumulator[T <: ScoredObject[T]]
    extends LinearSeq[T]
    with Builder[T, List[T]] {

  /** This operation is generally unsupported, as it can be difficult
    * to reach a particular index when the accumulator is of unknown
    * size.
    */
  final override def update(idx: Int, elem: T): Unit =
    throw new UnsupportedOperationException(s"Ha! You wish.")

  /** True if this accumulator has a definite size.
    * Default implementation is false. Override in subclasses.
    */
  def hasLimit: Boolean = false

  /** True if the number of elements in the accumulator is equal
    * to the limit, if one exists. Default implementation is false.
    */
  def atCapacity: Boolean = false
}
