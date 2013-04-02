package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.ExtentIterator
import edu.umass.ciir.julien.Aliases._

object Term { def apply(s: String) = new Term(s) }

/** Represents a direct connection to an index via a key specified
  * the t parameter. Each Term object carries references to the
  * underlying iterator the Term represents, as well as a reference
  * to the index that generated the iterator.
  */
class Term(val t: String) extends Value with Operator {
  // The index attached to this Term
  private[this] var index: Option[Index] = None

  // The iterator attached to this Term
  private[this] var iter: Option[ExtentIterator] = None

  def children: Seq[Operator] = List.empty
  def isAttached: Boolean = index.isDefined
  def attach(i: Index): Unit = {
    iter = Some(i.iterator(t))
    index = Some(i)
  }

  def underlying: ExtentIterator = {
    assume(iter.isDefined, s"Tried to use iterator of $t before attaching")
    iter.get
  }

  def attachedIndex: Index = {
    assume(index.isDefined, s"Tried to use index of $t before attaching")
    index.get
  }
}
