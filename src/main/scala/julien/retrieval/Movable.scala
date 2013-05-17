package julien
package retrieval

/** Trait that indicates explicit movement control is
  * necessary. Currently this requires a bounded data source,
  * but that assumption seems restrictive.
  */
trait Movable extends Bounded {
  def matches(id: Int): Boolean = true
  def reset: Unit
  def isDone: Boolean
  def at: Int
  def moveTo(id: Int): Boolean
  def movePast(id: Int): Int
}
