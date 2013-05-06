package julien.retrieval

import julien.InternalId

/**
 * User: jdalton
 * Date: 5/3/13
 */
class LengthsArray(val lengths : Array[Int]) extends RandomAccessHook[Int] {

  var curDoc: InternalId = _

  def get: Int = lengths(curDoc.underlying)
}
