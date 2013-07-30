package julien
package retrieval

import julien.galago.core.index.DataIterator

object DocumentView {
  def apply()(implicit index: Index) = new DocumentView(index)
}

final class DocumentView private(override val index: Index)
    extends IteratedHook[DataIterator[GDoc]]
    with DataView[Document] {
  override val underlying = index.documentIterator()

  override def toString: String =
    "DocumentView: " + index.toString

  // Feel like an unchecked version would be faster...
  def data(id: Int): Document = {
    underlying.syncTo(id)
    if (underlying.hasMatch(id)) DocumentClone(underlying.getData)
    else Document.empty
  }
}
