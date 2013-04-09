package julien
package access

/** Behavior needed by anything that connects to an index.
  * This trait also acts as a marker to indicate that
  * an attachment call is needed for the hook can be used.
  */
trait IndexHook {
  /** The index attached to this hook. */
  protected[this] var i: Option[Index] = None

  protected def index: Option[Index] = i
  protected def index_=(idx: Index) = i = Some(idx)

  /** Accessor for the attached index, if present. */
  def attachedIndex: Index = {
    assume(index.isDefined,
      s"Tried to use index ${toString} of before attaching")
    index.get
  }

  /** Attaches the provided index to this hook. Implementing
    * classes may add functionality to this method.
    */
  def attach(i: Index) { index = i }

  def isAttached: Boolean = index.isDefined
}
