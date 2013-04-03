package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.LengthsReader._

object LengthsOp { def apply() = new LengthsOp }

class LengthsOp
    extends ViewOp
    with ChildlessOperator
    with LengthsSrc
    with IteratedHook[LengthsIterator] {
  override def toString =
    s"lengths:" + (if (isAttached) index.toString else "")
  def getIterator(i: Index): LengthsIterator = i.lengthsIterator
  def length: Length = new Length(it.get.getCurrentLength)
}
