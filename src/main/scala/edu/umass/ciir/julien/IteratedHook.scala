package edu.umass.ciir.julien

trait IteratedHook[I <: GIterator] extends IndexHook {
  // The iterator attached to this hook
  protected[this] var it: Option[I] = None

  protected def iter: Option[I] = it
  protected def iter_=(it: I) = this.it = Some(it)

  protected def getIterator(i: Index): I

  override def attach(i: Index) {
    super.attach(i)
    iter = getIterator(i)
  }

  def underlying: I = {
    assume(iter.isDefined,
      s"Tried to use iterator of ${toString} before attaching")
    iter.get
  }
}
