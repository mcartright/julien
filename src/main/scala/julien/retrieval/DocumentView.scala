package julien
package retrieval

import org.lemurproject.galago.core.index.DataIterator

object DocumentView {
  def apply() = new DocumentView()
}

final class DocumentView private()
    extends IteratedHook[DataIterator[GDoc]]
    with DataView[Document] {

  override def toString: String =
    "DocumentView: " + (if (isAttached) index.toString else "")

  def getIterator(i: Index): DataIterator[GDoc] = i.documentIterator()
  def data: Document = DocumentClone(underlying.getData)
}
