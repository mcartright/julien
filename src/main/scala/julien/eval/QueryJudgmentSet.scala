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

  def fromTrec(src: String, useBinary: Boolean = false): QueryJudgmentSet = {
    val reader = Source.fromFile(src).bufferedReader
    val builder = scala.collection.mutable.HashMap[String, QueryJudgment]()
    val LinePattern = """(\w+) 0 (.+) ([0-9])"""".r
    while(reader.ready) {
      reader.readLine match {
        case LinePattern(qid, docid, label) => {
          if (!builder.contains(qid)) builder(qid) = new QueryJudgment(qid)
          builder(qid).put(docid, label.toInt)
        }
        case _ => // Nothing
      }
    }
    reader.close
    new QueryJudgmentSet(builder.toMap)
  }
}

class QueryJudgmentSet(
queryMap: Map[String, QueryJudgment]
) extends MapProxy[String, QueryJudgment] {
  def self = queryMap
  def numPrefPairs: Int = queryMap.mapValues(_.numPrefPairs).sum
}
