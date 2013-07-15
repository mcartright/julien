package julien
package behavior

/** Trait to indicate that something has finite length,
  * and whether it is defined for every value between its
  * lower and upper extrema (if yes, then 'isDense' should be true).
  */
trait Finite {
  /** The number of entries this View has values for. */
  def size: Int

  /** True if this View has values for every key in the universe of the
    * index.
    */
  def isDense: Boolean

  /** Is true if this View does *not* (or cannot be assumed to) have values
    * for every key in the universe of the index.
    */
  def isSparse: Boolean = !isDense
}
