package julien
package access

import julien.galago.core.index.CountIterator

class CountPosting protected (var docid: Docid, var count: Int)
    extends Posting[CountPosting]
    with CountSrc {
  def copy: CountPosting = CountPosting(this)
}

object CountPosting {
  val thePosting = new CountPosting(Docid(0), 0)
  def apply(cp: CountPosting) = new CountPosting(cp.docid, cp.count)
  def apply(d: Docid, c: Int) = new CountPosting(d, c)
  implicit def apply(e: CountIterator) = {
    thePosting.docid = Docid(e.currentCandidate)
    thePosting.count = e.count
    thePosting
  }
}
