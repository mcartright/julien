package edu.umass.ciir.julien

import scala.collection.mutable.{PriorityQueue,LinkedHashMap}
import scala.collection.Map
import scala.collection.JavaConversions._

import org.lemurproject.galago.core.index.{Index, AggregateReader}
import org.lemurproject.galago.core.index.LengthsReader
import org.lemurproject.galago.core.index.Iterator
import org.lemurproject.galago.core.index.NullExtentIterator
import org.lemurproject.galago.core.index.ExtentIterator
import org.lemurproject.galago.core.index.CountIterator
import org.lemurproject.galago.core.index.disk.DiskIndex
import org.lemurproject.galago.core.index.disk.PositionIndexReader
import org.lemurproject.galago.core.parse.Document

import org.lemurproject.galago.tupleflow.{Parameters,Utility}

import Utils._

object ExecutionEnvironment {

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
