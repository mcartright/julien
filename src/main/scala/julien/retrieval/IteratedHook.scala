package julien
package retrieval

/** Indicates behavior of an [[IndexHook]] that contains
  * an [[julien.galago.core.index.Iterator]]
  * as its underlying data source.
  */
trait IteratedHook[I <: GIterator]
    extends View
    with ChildlessOp
    with IndexHook
    with Movable {
  /** The iterator attached to this hook. */
  val underlying: I

  /** We have an iterator, so we can determine if its dense (has an entry
    * for every retrieval item in the collection).
    */
  override def isDense: Boolean = underlying.hasAllCandidates

  /** The estimated number of entries in this view. */
  override def size: Int = underlying.totalEntries.toInt
  def reset: Unit = underlying.reset
  def isDone: Boolean = underlying.isDone
  def at: Int = underlying.currentCandidate
  def moveTo(id: Int) = underlying.syncTo(id)
  def movePast(id: Int) = underlying.movePast(id)
}
