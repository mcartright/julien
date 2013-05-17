package julien
package retrieval

import julien.galago.core.util.ExtentArray

/** An extension to the PositonsView. The method here indicates
  * that the providing view can provide a single ExtentArray,
  * and as it moves, it keeps that reference up to date.
  *
  * This allows any client view to ask for the buffer once
  * and simply check the held reference as the underlying view
  * progresses.
  *
  * This optimization only works with a streaming view: note that in-memory
  * views don't need to do this, as all of the positions are already in memory.
  *
  * See [[julien.retrieval.OrderedWindow OrderedWindow]] and
  * [[julien.retrieval.UnorderedWindow UnorderedWindow]] for examples on usage
  * of trait.
  */
trait PositionsBufferView extends PositionsView {
  def positionsBuffer: ExtentArray
}
