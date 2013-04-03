package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.{Iterator,ExtentIterator}
import edu.umass.ciir.julien.Aliases._

object Term { def apply(s: String) = new Term(s) }

/** Represents a direct connection to an index via a key specified
  * the t parameter. Each Term object carries references to the
  * underlying iterator the Term represents, as well as a reference
  * to the index that generated the iterator.
  *
  * These are always 1-to-1.
  */
final class Term(val t: String)
    extends ChildlessOperator
    with IteratedHook[ExtentIterator] {
  override def toString: String =
    s"$t:" + (if (isAttached) index.toString else "")
  def getIterator(i: Index): ExtentIterator = i.iterator(t)
}

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

trait IteratedHook[I <: Iterator] extends IndexHook {
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
