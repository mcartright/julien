package julien
package eval

import scala.collection.SeqProxy

// Let's see if this works... if so, should probably mixin IterableProxyLike
case class QueryResult[T <: ScoredObject[T](val name: String, result: List[T])
extends SeqProxy[T] {
  def self = result
}
