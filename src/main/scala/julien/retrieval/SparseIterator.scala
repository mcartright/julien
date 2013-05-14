package julien
package retrieval

trait SparseIterator[I <: GIterator] extends IteratedHook[I] {
  protected var matched = true // start off being matched to the first doc

  override def matches(id: Int): Boolean = underlying.hasMatch(id)
  override def moveTo(id: Int) {
    underlying.syncTo(id)
    matched = underlying.hasMatch(id)
  }

  override def movePast(id: Int) {
    underlying.movePast(id)
    matched = underlying.hasMatch(id)
  }
}
