package edu.umass.ciir.julien

trait IndexHook {
  // The index attached to this Term
  protected[this] var i: Option[Index] = None
  protected def index: Option[Index] = i
  protected def index_=(idx: Index) = i = Some(idx)

  def attachedIndex: Index = {
    assume(index.isDefined,
      s"Tried to use index ${toString} of before attaching")
    index.get
  }
  def attach(i: Index) { index = i }
  def isAttached: Boolean = index.isDefined
}
