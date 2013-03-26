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
import TermScorers._
import Intersections._

object ExecutionGraph {
  type ARCA = AggregateReader.CollectionAggregateIterator
  type ARNA = AggregateReader.NodeAggregateIterator
  type TEI = ExtentIterator
  type TCI = CountIterator
  type MLI = LengthsReader.LengthsIterator

  implicit def lengths2ARCA(l: MLI) = l.asInstanceOf[ARCA]
  implicit def ARCA2lengths(l: ARCA) = l.asInstanceOf[MLI]
  implicit def term2key(n : Term) = Utility.fromString(n.text)
}

class ExecutionGraph(graph: QueryGraph) {
  import ExecutionGraph._

  // prepare the needed components to run
  val index = graph.index match {
  case Some(name) => Sources.get(name)
  case None => throw new IllegalArgumentException("No default index specified!")
  }
  val dummy = new Parameters()
  val lengths = index.getLengthsIterator
  val nodeMap = LinkedHashMap[Term, TEI]()
  for (n <- graph.leaves) {
    nodeMap.update(n,
      index.getIterator(n, dummy).asInstanceOf[ExtentIterator])
  }
  val iterators = nodeMap.values.toList
  val scorers = graph.scoreNodes.map(transform(_))

  private def transform(sn:ScoreNode) : ParameterizedScorer = sn match {
    case f: Features =>
      ParameterizedScorer(f.features, f.weights, f.scorer, lengths, nodeMap(f))
    case t: Term => ParameterizedScorer(t.scorer, lengths, nodeMap(t))
    case c: Combine => {
      val scorers = c.children.map(transform(_))
      ParameterizedScorer(scorers, c.combiner)
    }
    case i: Intersected =>
      ParameterizedScorer(
        i.scorer,
        i.filter,
        lengths,
        i.children.map(n => nodeMap(n))
      )
  }


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

  def collectionCount(node: Term, index: Index) : Double = {
    val it = index.getIterator(node, dummy)
    it match {
      case _: NullExtentIterator => 0.5 // smooth it
      case _ => {
        val stats = it.asInstanceOf[ARNA].getStatistics()
        stats.nodeFrequency.toDouble
      }
    }
  }

  def collectionLength(index: Index) : Long = {
    val lengths = index.getLengthsIterator.asInstanceOf[ARCA]
    lengths.getStatistics.collectionLength
  }

  def avgLength(index: Index) : Double = {
    val lengths = index.getLengthsIterator.asInstanceOf[ARCA]
    lengths.getStatistics.avgLength
  }

  def numDocuments(index: Index) : Long = {
    val lengths = index.getLengthsIterator.asInstanceOf[ARCA]
    lengths.getStatistics.documentCount
  }

  def collectionFrequency(node: Term, index: Index): Double = {
    collectionCount(node, index) / collectionLength(index)
  }

  def documentCount(node: Term, index: Index) : Double = {
    val it = index.getIterator(node, dummy)
    it match {
      case _: NullExtentIterator => 0.5
      case _ => {
        val stats = it.asInstanceOf[ARNA].getStatistics()
        stats.nodeDocumentCount.toDouble
      }
    }
  }

  def unigrams(
    index: DiskIndex,
    lengths : ARCA
  ) : List[ParameterizedScorer] = {
    nodeMap.keys.map { Q =>
      val scorer = dirichlet(collectionFrequency(Q, index))
      ParameterizedScorer(scorer, lengths, nodeMap(Q))
    }.toList
  }

  def orderedWindows(
    index: DiskIndex,
    lengths: ARCA,
    width: Int = 1
  ) : List[ParameterizedScorer] = {
    nodeMap.keys.toList.sliding(2,1).map { termPair =>
      val scorer = dirichlet(collectionFrequency(termPair(0), index))
      val isect = od(width)
      val iterators = termPair.map(nodeMap(_))
      ParameterizedScorer(scorer, isect, lengths, iterators)
    }.toList
  }

  def unorderedWindows(
    index: DiskIndex,
    lengths: ARCA,
    width: Int = 1
  ) : List[ParameterizedScorer] = {
    nodeMap.keys.toList.sliding(2,1).map { termPair =>
      val scorer = dirichlet(collectionFrequency(termPair(0), index))
      val isect = uw(width)
      val iterators = termPair.map(nodeMap(_))
      ParameterizedScorer(scorer, isect, lengths, iterators)
    }.toList
  }
}
