package julien
package eval

import scala.io.Source
import scala.collection.MapProxy

// Gonna need some reflection to get the right object type to
// build. However this should just do it at the line level
object QueryJudgmentSet {
  def apply(src: String, isBinary: Boolean = false): QueryJudgmentSet = {
    // TODO - IMPLEMENT ME
    new QueryJudgmentSet(Map.empty[String, QueryJudgment])
  }
}

class QueryJudgmentSet(
queryMap: Map[String, QueryJudgment]
) extends MapProxy[String, QueryJudgment] {
  def self = queryMap
}
