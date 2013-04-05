package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.{ExtentIterator,ScoreIterator}

/** Encapsulation of a single posting in the index.
  * An Index object can produce SeqViews over these
  * given a ViewOp.
  */
trait Posting[T <: Posting[T]] {
  def docid: Docid
  def copy: T
}

class CountPosting protected (var docid: Docid, var count: Count)
    extends Posting[CountPosting]
    with CountSrc {
  def copy: CountPosting = CountPosting(this)
}

object CountPosting {
  val thePosting = new CountPosting(Docid(0), Count(0))
  def apply(cp: CountPosting) = new CountPosting(cp.docid, cp.count)
  def apply(d: Docid, c: Count) = new CountPosting(d, c)
  implicit def apply(e: ExtentIterator) = {
    thePosting.docid = Docid(e.currentCandidate)
    thePosting.count = Count(e.count)
    thePosting
  }
}

class PositionsPosting protected (
  var docid: Docid,
  var count: Count,
  var positions: Positions)
    extends Posting[PositionsPosting]
    with PositionSrc {
  def copy: PositionsPosting = PositionsPosting(this)
}

object PositionsPosting {
  val thePosting = new PositionsPosting(Docid(0), Count(0), Positions())
  def apply(p: PositionsPosting) =
    new PositionsPosting(p.docid, p.count, p.positions)
  def apply(d: Docid, c: Count, p: Positions) = new PositionsPosting(d, c, p)
  implicit def apply(e: ExtentIterator) = {
    thePosting.docid = Docid(e.currentCandidate)
    thePosting.count = Count(e.count)
    thePosting.positions = Positions(e.extents)
    thePosting
  }
}

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
// trait DataPosting extends Posting with DataSrc
