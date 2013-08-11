package julien
package eval

import scala.io.Source
import scala.collection.MapProxy
import collection.mutable.ListBuffer

// Gonna need some reflection to get the right object type to
// build. However this should just do it at the line level
object QueryJudgmentSet {

  def fromTrec(src: String, useBinary: Boolean = false): QueryJudgmentSet = {
    val reader = Source.fromFile(src).bufferedReader

    val entries = List.newBuilder[(String, String, Int)]
    while(reader.ready) {
      val line = reader.readLine
      val columns = line.split("\\s+")
      val queryId = columns(0)
      val docId = columns(2)
      val relevance = columns(3).toInt
      entries += Tuple3(queryId, docId, relevance)
    }
    reader.close
    // Map[QueryId, tuple]
    val grouped = entries.result.groupBy(item => item._1)
    // Map[QueryId, QueryJudgments]
    val remapped = grouped.mapValues { items =>
      val qj = QueryJudgments(items.head._1)
      for (it <- items) qj.update(it._2, it._3)
      qj
    }
    apply(remapped)
  }

  def apply(jmap: Map[String, QueryJudgments]): QueryJudgmentSet =
    new QueryJudgmentSet(jmap)
}

class QueryJudgmentSet(
  jMap: Map[String, QueryJudgments]
)
    extends MapProxy[String, QueryJudgments]
{
  def self = jMap
}
