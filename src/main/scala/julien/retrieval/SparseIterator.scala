package julien
package retrieval

/** A behavior indicating that the implementing class:
  * - Is stream-based,
  * - Directly accesses an index segment (most likely a posting list),
  * - And is not expected to have a non-degenerate value for every
  *   identifier in the index.
  */
trait SparseIterator[I <: GIterator] extends IteratedHook[I] {
  override def matches(id: Int): Boolean = underlying.hasMatch(id)
  override def moveTo(id: Int) = underlying.syncTo(id)
  override def movePast(id: Int) = underlying.movePast(id)
}
