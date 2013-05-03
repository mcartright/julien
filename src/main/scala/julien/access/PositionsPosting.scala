package julien
package access

import julien.galago.core.index.ExtentIterator
import julien.galago.core.util.ExtentArray
class PositionsPosting protected (
  var docid: Docid,
  var count: Int,
  var positions: ExtentArray)
    extends Posting[PositionsPosting]
    with PositionSrc {
  def copy: PositionsPosting = PositionsPosting(this)
}

object PositionsPosting {
  val thePosting = new PositionsPosting(Docid(0), 0, ExtentArray.empty)
  def apply(p: PositionsPosting) =
    new PositionsPosting(p.docid, p.count, p.positions)
  def apply(d: Docid, c: Int, p: ExtentArray) = new PositionsPosting(d, c, p)
  implicit def apply(e: ExtentIterator) = {
    thePosting.docid = Docid(e.currentCandidate)
    thePosting.count = e.count
    thePosting.positions = e.extents
    thePosting
  }
}
