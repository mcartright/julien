package julien
package retrieval

import julien.galago.core.index.ScoreIterator
import julien.access._

// Simple class that grabs stored priors and presents them as a hybrid
// feature/view.
class PriorsView
    extends SparseIterator[ScoreIterator]
    with ScalarWeightedFeature {
  def getIterator(i: Index): ScoreIterator = {
    i.partReader("priors").getIterator("priors").asInstanceOf[ScoreIterator]
  }

  lazy val views: Set[ViewOp] = Set[ViewOp](this)
  def eval: Double = underlying.score
  def score: Double = underlying.score
}
