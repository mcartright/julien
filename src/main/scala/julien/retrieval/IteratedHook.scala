package julien
package retrieval

/** Indicates behavior of an [[IndexHook]] that contains
  * an [[julien.galago.core.index.Iterator]]
  * as its underlying data source.
  */
trait IteratedHook[I <: GIterator]
    extends ViewOp
    with ChildlessOp
    with IndexHook {
  /** The iterator attached to this hook. */
  protected[this] var it: Option[I] = None

  /** Iterator getter. This is for internal use only. */
  protected def getIterator(i: Index): I

  /** Overriden to add functionality to additionally attach
    * the iterator used in providing data.
    */
  override def attach(i: Index) {
    super.attach(i)
    it = Some(getIterator(i))
  }

  /** Returns the underlying iterator. If it is not defined,
    * an assertion fail is thrown. So, attach this hook before
    * using it. Doy.
    */
  def underlying: I = {
    assume(it.isDefined,
      s"Tried to use iterator of ${toString} before attaching")
    it.get
  }

  /** We have an iterator, so we can determine if its dense (has an entry
    * for every retrieval item in the collection).
    */
  override def isDense: Boolean = it.get.hasAllCandidates

  /** The estimated number of entries in this view.
    */
  override def size: Int = it.get.totalEntries.toInt


  /** This should be refactored into a DenseIterator */
  def matches(id: Int): Boolean = true
  @inline def isDone: Boolean = underlying.isDone
  @inline def at: Int = underlying.currentCandidate
  @inline def moveTo(id: Int) = underlying.syncTo(id)
  @inline def movePast(id: Int) = underlying.movePast(id)
  @inline def totalEntries: Long = underlying.totalEntries
}
