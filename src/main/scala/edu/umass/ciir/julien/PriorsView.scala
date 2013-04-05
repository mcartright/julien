package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.ScoreIterator

// Simple class that grabs stored priors and presents them as a hybrid
// feature/view.
class PriorsView
    extends FeatureView
    with IteratedHook[ScoreIterator] {

  def getIterator(i: Index): ScoreIterator = {
    i.partReader("priors").getIterator("priors").asInstanceOf[ScoreIterator]
  }
  lazy val views: Set[ViewOp] = Set[ViewOp](this)
  def eval: Score = Score(it.get.score)
}
