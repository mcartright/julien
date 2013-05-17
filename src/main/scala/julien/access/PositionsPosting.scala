package julien
package access

import julien.galago.core.index.ExtentIterator
import julien.galago.core.util.ExtentArray
class PositionsPosting protected (
  var docid: InternalId,
  var count: Int,
  var positions: ExtentArray)
    extends Posting[PositionsPosting] {
  def copy: PositionsPosting = PositionsPosting(this)
}

object PositionsPosting {
  import language.implicitConversions

  val thePosting = new PositionsPosting(InternalId(0), 0, ExtentArray.empty)
  def apply(p: PositionsPosting) =
    new PositionsPosting(p.docid, p.count, p.positions)
  def apply(d: InternalId, c: Int, p: ExtentArray) = new PositionsPosting(d, c, p)
  implicit def apply(e: ExtentIterator) = {
    thePosting.docid = InternalId(e.currentCandidate)
    thePosting.count = e.count
    thePosting.positions = e.extents
    thePosting
  }
}
