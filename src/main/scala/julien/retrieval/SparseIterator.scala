package julien
package retrieval

trait SparseIterator[I <: GIterator] extends IteratedHook[I] {
  override def matches(id: Int): Boolean = underlying.hasMatch(id)
  override def moveTo(id: Int) = underlying.syncTo(id)
  override def movePast(id: Int) = underlying.movePast(id)
}
