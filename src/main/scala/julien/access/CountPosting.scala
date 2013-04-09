package julien

import org.lemurproject.galago.core.index.CountIterator

class CountPosting protected (var docid: Docid, var count: Count)
    extends Posting[CountPosting]
    with CountSrc {
  def copy: CountPosting = CountPosting(this)
}

object CountPosting {
  val thePosting = new CountPosting(Docid(0), Count(0))
  def apply(cp: CountPosting) = new CountPosting(cp.docid, cp.count)
  def apply(d: Docid, c: Count) = new CountPosting(d, c)
  implicit def apply(e: CountIterator) = {
    thePosting.docid = Docid(e.currentCandidate)
    thePosting.count = Count(e.count)
    thePosting
  }
}
