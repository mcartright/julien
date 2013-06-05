package julien
package retrieval

import julien.galago.core.index.ScoreIterator
import julien.access._

object PriorsView {
  // This should really try to pre-load based on segment size.
  // TODO - make a memory version
  def apply(default: Double = 0)(implicit index: Index) =
    new PriorsView(index, default)
}

// Simple class that grabs stored priors and presents them as a hybrid
// feature/view.
class PriorsView private(override val index: Index, val defaultValue: Double)
    extends SparseIterator[ScoreIterator]
    with ScalarWeightedFeature {
  override val underlying =
    index.partReader("priors").getIterator("priors").asInstanceOf[ScoreIterator]

  lazy val views: Set[View] = Set[View](this)
  def eval(id: InternalId): Double = {
    underlying.syncTo(id)
    if (underlying.hasMatch(id)) underlying.score
    else defaultValue
  }
}
