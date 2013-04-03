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
    extends IteratedHook[ExtentIterator]
    with Operator {
  override def toString: String = s"`$t"
  def children: Seq[Operator] = List.empty


  private[this] var i: Option[Index] = None
  private[this] var ei: Option[ExtentIterator] = None

  def index: Option[Index] = i
  def index_=(idx: Index) = i = Some(idx)

  def iter: Option[ExtentIterator] = ei
  def iter_=(it: ExtentIterator) = ei = Some(it)

  def getIterator(i: Index): ExtentIterator = i.iterator(t)
}

sealed trait IndexHook {
  // The index attached to this Term
  protected def index: Option[Index]
  protected def index_=(i: Index): Unit

  def attachedIndex: Index = {
    assume(index.isDefined,
      s"Tried to use index ${toString} of before attaching")
    index.get
  }
  def attach(i: Index) { index = i }
  def isAttached: Boolean = index.isDefined
}

sealed trait IteratedHook[I <: Iterator] extends IndexHook {
  // The iterator attached to this hook
  protected def iter: Option[I]
  protected def iter_=(i: I): Unit
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
