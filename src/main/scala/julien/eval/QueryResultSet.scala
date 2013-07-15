package julien
package eval

import scala.io.Source
import scala.collection.MapProxy
import scala.util.Random
import scala.math._

object QueryResultSet {
  def fromTrec[T <: ScoredObject](src: String): QueryResultSet[T] = {
    val reader = Source.fromFile(src).bufferedReader
    // TODO: This is not the right pattern. Seriously - I think it's for
    // qrels. This needs to read in run results, if we use it at all.
    val LinePattern = """(\w+) 0 (.+) ([0-9])"""".r
    while(reader.ready) {
      reader.readLine match {
        case LinePattern(qid, docid, label) => {
          // Need to implement something useful in here.
        }
        case _ => // Nothing
      }
    }
    reader.close

    new QueryResultSet(Map.empty[String, QueryResult[T]])
  }

  def apply[T <: ScoredObject](
    input: Map[String, QueryResult[T]]
  ): QueryResultSet[T] = new QueryResultSet(input)
}

class QueryResultSet[T <: ScoredObject](
  queryMap: Map[String, QueryResult[T]]
) extends MapProxy[String, QueryResult[T]] {
  def self = queryMap

  def sample(sampleRate: Double): QueryResultSet[T] = {
    assume(sampleRate > 0.0 && sampleRate <= 1.0,
      s" Can't sample with a rate of $sampleRate")

    if (sampleRate == 1.0) return QueryResultSet(queryMap)
    else {
      val newSize = round(queryMap.size * sampleRate).toInt
      QueryResultSet(Random.shuffle(queryMap).take(newSize).toMap)
    }
  }
}
