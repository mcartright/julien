package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.ExtentIterator

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
