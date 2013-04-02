package sources

import scala.collection.mutable.{PriorityQueue,LinkedHashMap}
import scala.collection.Map
import scala.collection.JavaConversions._

import org.lemurproject.galago.core.index.{Index, AggregateReader}
import org.lemurproject.galago.core.index.LengthsReader
import org.lemurproject.galago.core.index.Iterator
import org.lemurproject.galago.tupleflow.{Parameters,Utility}

import edu.umass.ciir.julien.Utils._
import edu.umass.ciir.julien.{ScoredDocument, ScoredDocumentOrdering}

abstract class ExecutionEnvironment {
  val iterators : List[Iterator] = List.empty
  val scorers: List[QueryVariable] = List.empty
  val lengths : LengthsReader.LengthsIterator

  def run(numResults: Int = 100) : PriorityQueue[ScoredDocument] = {
    val resultQueue = PriorityQueue[ScoredDocument]()(ScoredDocumentOrdering)
    while (iterators.exists(_.isDone == false)) {
      val candidate = iterators.filterNot(_.isDone).map(_.currentCandidate).min
      lengths.syncTo(candidate)
      iterators.foreach(_.syncTo(candidate))
      if (iterators.exists(_.hasMatch(candidate))) {
        // Time to score
        var score = scorers.foldRight(0.0)((S,N) => S.score + N)
        resultQueue.enqueue(ScoredDocument(candidate, score))
        if (resultQueue.size > numResults) resultQueue.dequeue
      }
      iterators.foreach(_.movePast(candidate))
    }
    return resultQueue.reverse
  }
}
