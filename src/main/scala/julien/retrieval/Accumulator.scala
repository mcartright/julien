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

  // Need overriding
  def hasLimit: Boolean = false
}
