package julien
package access

import julien.galago.core.util.ExtentArray

case class PositionsPosting(
  var docid: InternalId,
  var count: Int,
  var positions: ExtentArray)
