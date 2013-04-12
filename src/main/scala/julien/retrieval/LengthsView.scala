package julien
package retrieval

import org.lemurproject.galago.core.index.LengthsReader._

object LengthsView { def apply() = new LengthsView }

class LengthsView
    extends IteratedHook[LengthsIterator]
    with LengthsSrc {
  override def toString =
    s"lengths:" + (if (isAttached) index.toString else "")
  def getIterator(i: Index): LengthsIterator = i.lengthsIterator
  def length: Length = new Length(it.get.getCurrentLength)

}
