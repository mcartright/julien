package julien
package eval

import scala.io.Source

// Gonna need some reflection to get the right object type to
// build. However this should just do it at the line level
object QueryJudgmentSet {
  def apply[T <: ScoredObject[T]](src: String): QueryJudgmentSet[T] = {
    // TODO - IMPLEMENT ME
  }
}

class QueryJudgmentSet[T <: ScoredObject[T]](
queryMap: Map[String, QueryJudgment[T]]
) extends MapProxy[String, QueryJudgment[T]] {
  def self = queryMap
}
