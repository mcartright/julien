package julien
package retrieval

/** Trait to indicate that something has finite length,
  * and whether it is defined for every value between its
  * lower and upper extrema (if yes, then 'isDense' should be true).
  */
trait Bounded {
  def size: Int
  def isDense: Boolean
  def isSparse: Boolean = !isDense
}
