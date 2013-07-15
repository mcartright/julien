package julien
package retrieval

import scala.collection.mutable.{Iterable, Builder}

/** This trait defines what an accumulator needs to behave like.
  * Basically, it's some sort of Buffer (we should be able to mutate it),
  * but it's also iterable - meaning yu can iterate over the contents of the
  * accumulator. Iteration order is assumed to be stable (i.e. the contents
  * of the accumulator are somehow ordered).
  */
trait Accumulator[T <: ScoredObject]
    extends Iterable[T]
    with Builder[T, List[T]] {

  /** If a limit for the accumulator is given, should return that value in an
    * Some(Int). Otherwise, return a None, indicating the accumulator is
    * effectively "bottomless".
    */
  def limit: Option[Int] = None

  /** True if the number of elements in the accumulator is equal
    * to the limit, if one exists. Default implementation is false.
    */
  def atCapacity: Boolean = false
}
