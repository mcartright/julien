package julien
package access

import julien.galago.core.index.CountIterator

import language.implicitConversions

class CountPosting protected (var docid: InternalId, var count: Int)
    extends Posting[CountPosting] {
  def copy: CountPosting = CountPosting(this)
}

object CountPosting {
  val thePosting = new CountPosting(InternalId(0), 0)
  def apply(cp: CountPosting) = new CountPosting(cp.docid, cp.count)
  def apply(d: InternalId, c: Int) = new CountPosting(d, c)
  implicit def apply(e: CountIterator) = {
    thePosting.docid = InternalId(e.currentCandidate)
    thePosting.count = e.count
    thePosting
  }
}
