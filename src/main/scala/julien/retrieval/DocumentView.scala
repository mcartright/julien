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
  def data: Document = DocumentClone(underlying.getData)
}
