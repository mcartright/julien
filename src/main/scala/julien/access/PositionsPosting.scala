package julien
package access

import julien.galago.core.util.ExtentArray
import scala.math.Ordered

case class PositionsPosting(
  var term: String,
  var docid: InternalId,
  var count: Int,
  var positions: ExtentArray)
    extends KeyedData
    with Ordered[PositionsPosting] {
  def key = term
  def compare(that: PositionsPosting): Int = {
    var r = this.term compare that.term
    if (r != 0) return r
    this.docid - that.docid
  }
}
