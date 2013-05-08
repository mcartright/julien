
package julien
package retrieval

trait Bounded {
  def size: Int
  def isDense: Boolean
  def isSparse: Boolean = !isDense
}

trait Movable extends Bounded {
  def matches(id: Int): Boolean = true
  def reset: Unit
  def isDone: Boolean
  def at: Int
  def moveTo(id: Int)
  def movePast(id: Int)
}
