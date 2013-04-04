package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.{ExtentIterator,ScoreIterator}

/** Encapsulation of a single posting in the index.
  * An Index object can produce SeqViews over these
  * given a ViewOp.
  */
trait Posting { var docid = new Docid(0) }

class CountPosting extends Posting with CountSrc {
  var count: Count = new Count(0)
}

object CountPosting {
  val thePosting = new CountPosting
  implicit def apply(e: ExtentIterator) = {
    thePosting.docid = Docid(e.currentCandidate)
    thePosting.count = Count(e.count)
    thePosting
  }
}

class PositionsPosting extends CountPosting with PositionSrc {
  var positions: Positions = Positions()
}

object PositionsPosting {
  val thePosting = new PositionsPosting
  implicit def apply(e: ExtentIterator) = {
    thePosting.docid = Docid(e.currentCandidate)
    thePosting.count = Count(e.count)
    thePosting.positions = Positions(e.extents)
    thePosting
  }
}

class ScorePosting extends Posting with ScoreSrc {
  var score: Score = Score(0)
}

object ScorePosting {
  val thePosting = new ScorePosting
  implicit def apply(s: ScoreIterator) = {
    thePosting.docid = Docid(s.currentCandidate)
    thePosting.score = Score(s.score)
    thePosting
  }
}
// trait DataPosting extends Posting with DataSrc
