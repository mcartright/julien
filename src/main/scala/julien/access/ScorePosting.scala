package julien
package access

import julien.galago.core.index.ScoreIterator

class ScorePosting protected (var docid: InternalId, var score: Double)
    extends Posting[ScorePosting]
    with ScoreSrc {
  def copy: ScorePosting = ScorePosting(this)
}

object ScorePosting {
  val thePosting = new ScorePosting(InternalId(0), 0.0)
  def apply(p: ScorePosting) = new ScorePosting(p.docid, p.score)
  def apply(d: InternalId, s: Double) = new ScorePosting(d, s)
  implicit def apply(s: ScoreIterator) = {
    thePosting.docid = InternalId(s.currentCandidate)
    thePosting.score = s.score
    thePosting
  }
}
