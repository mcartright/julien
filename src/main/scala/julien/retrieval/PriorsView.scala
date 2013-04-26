package julien
package retrieval

import org.lemurproject.galago.core.index.ScoreIterator
import julien.access._

// Simple class that grabs stored priors and presents them as a hybrid
// feature/view.
class PriorsView
    extends IteratedHook[ScoreIterator]
    with ScalarWeightedFeature {
  def getIterator(i: Index): ScoreIterator = {
    i.partReader("priors").getIterator("priors").asInstanceOf[ScoreIterator]
  }

  lazy val views: Set[ViewOp] = Set[ViewOp](this)
  def eval: Double = it.get.score
  def score: Double = it.get.score
}
