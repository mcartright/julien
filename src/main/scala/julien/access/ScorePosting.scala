package julien

import org.lemurproject.galago.core.index.ScoreIterator

class ScorePosting protected (var docid: Docid, var score: Score)
    extends Posting[ScorePosting]
    with ScoreSrc {
  def copy: ScorePosting = ScorePosting(this)
}

object ScorePosting {
  val thePosting = new ScorePosting(Docid(0), Score(0))
  def apply(p: ScorePosting) = new ScorePosting(p.docid, p.score)
  def apply(d: Docid, s: Score) = new ScorePosting(d, s)
  implicit def apply(s: ScoreIterator) = {
    thePosting.docid = Docid(s.currentCandidate)
    thePosting.score = Score(s.score)
    thePosting
  }
}
