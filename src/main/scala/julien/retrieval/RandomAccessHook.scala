package julien.retrieval

import julien.InternalId

/**
 * User: jdalton
 * Date: 5/3/13
 */
trait RandomAccessHook[T] {

  var curDoc:InternalId

  def get : T


}
