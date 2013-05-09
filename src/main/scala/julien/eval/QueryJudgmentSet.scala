package julien
package eval

import scala.io.Source
import scala.collection.MapProxy
import collection.mutable.ListBuffer

// Gonna need some reflection to get the right object type to
// build. However this should just do it at the line level
object QueryJudgmentSet {
//  def apply(src: String, isBinary: Boolean = false): QueryJudgmentSet = {
//    // TODO - IMPLEMENT ME
//  //  new QueryJudgmentSet(Map.empty[String, Map.empty[String, Seq[RelevanceJudgment]])
//  }

  def fromTrec(src: String, useBinary: Boolean = false): QueryJudgmentSet = {
    val reader = Source.fromFile(src).bufferedReader

    val buffer = new ListBuffer[RelevanceJudgment]
    while(reader.ready) {
      val line = reader.readLine
      val columns = line.split("\\s+")
      val queryId = columns(0)
      val docId = columns(2)
      val relevance = columns(3).toInt

      buffer += RelevanceJudgment(queryId, docId, relevance)
    }

    val queryJudgments = buffer.toSeq.groupBy(_.query).mapValues(_.groupBy(_.name).mapValues(_.head))
    reader.close
    queryJudgments
  }
}
