package julien
package eval

import scala.collection.MapProxy

object QueryResultSet {
  def fromTrec[T <: ScoredObject[T]](src: String): QueryResultSet[T] = {
    new QueryResultSet(Map.empty[String, QueryResult[T]])
  }

  def apply[T <: ScoredObject[T]](
    input: Map[String, QueryResult[T]]
  ): QueryResultSet[T] = new QueryResultSet(input)
}

class QueryResultSet[T <: ScoredObject[T]](
  queryMap: Map[String, QueryResult[T]]
) extends MapProxy[String, QueryResult[T]] {
  def self = queryMap
}
