package julien
package eval

object QueryResultSet {
  def fromTrec[T <: ScoredObject[T]](src: String): QueryResultSet[T] = {

  }
}

class QueryResultSet[T <: ScoredObject[T]](
  queryMap: Map[String, QueryResult[T]]
) extends MapProxy[String, QueryResult[T]] {
  def self = queryMap
}
