package julien
package access

/** Indicates behavior of an [[IndexHook]] that contains
  * an [[org.lemurproject.galago.core.index.Iterator]]
  * as its underlying data source.
  */
trait IteratedHook[I <: GIterator] extends IndexHook {
  /** The iterator attached to this hook. */
  protected[this] var it: Option[I] = None

  protected def iter: Option[I] = it
  protected def iter_=(it: I) = this.it = Some(it)

  /** Iterator getter. */
  protected def getIterator(i: Index): I

  /** Overriden to add functionality to additionally attach
    * the iterator used in providing data.
    */
  override def attach(i: Index) {
    super.attach(i)
    iter = getIterator(i)
  }

  /** Returns the underlying iterator. If it is not defined,
    *  an assertion fail is thrown. So, attach this hook before
    *  using it. Doy.
    */
  def underlying: I = {
    assume(iter.isDefined,
      s"Tried to use iterator of ${toString} before attaching")
    iter.get
  }
}
