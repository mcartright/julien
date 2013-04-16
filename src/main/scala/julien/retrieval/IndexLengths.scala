package julien
package retrieval

import org.lemurproject.galago.core.index.LengthsReader._

object IndexLengths { def apply() = new IndexLengths }

class IndexLengths
    extends IteratedHook[LengthsIterator]
    with LengthsView {
  override def toString =
    s"lengths:" + (if (isAttached) index.toString else "")
  def getIterator(i: Index): LengthsIterator = i.lengthsIterator
  def length: Length = new Length(it.get.getCurrentLength)
}
