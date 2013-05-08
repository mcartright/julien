package julien
package retrieval

/** Indicates behavior of an [[IndexHook]] that contains
  * an [[julien.galago.core.index.Iterator]]
  * as its underlying data source.
  */
trait IteratedHook[I <: GIterator]
    extends ViewOp
    with ChildlessOp
    with IndexHook
    with Movable {
  /** The iterator attached to this hook. */
  var underlying: I = _

  /** Iterator getter. This is for internal use only. */
  protected def getIterator(i: Index): I

  /** Overriden to add functionality to additionally attach
    * the iterator used in providing data.
    */
  override def attach(i: Index) {
    super.attach(i)
    underlying = getIterator(i)
  }

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
