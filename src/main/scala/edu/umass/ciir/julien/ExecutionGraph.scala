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
  val index = graph.defaultIndex match {
  case Some(name) => Sources.get(name)
  case None => throw new IllegalArgumentException("No default index specified!")
  }
  val dummy = new Parameters()
  val lengths = index.getLengthsIterator
  val nodeMap = LinkedHashMap[Node, TEI]()
  for (n <- graph.leaves) {
    nodeMap.update(n,
      index.getIterator(n, dummy).asInstanceOf[ExtentIterator])
  }
  val iterators = nodeMap.values.toList
  val scorers : graph.scoreNodes.map { n =>
    n match {
      case Term(text, p) =>
    }
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

  def collectionCount(node: Node, index: Index) : Double = {
    val it = index.getIterator(node)
    it match {
      case _: NullExtentIterator => 0.5 // smooth it
      case _ => {
        val stats = it.asInstanceOf[ARNA].getStatistics()
        stats.nodeFrequency.toDouble
      }
    }
  }

  def collectionLength(lengths: ARCA) : Long = {
    lengths.getStatistics.collectionLength
  }

  def avgLength(lengths: ARCA) : Double = {
    lengths.getStatistics.avgLength
  }

  def numDocuments(lengths: ARCA) : Long = {
    lengths.getStatistics.documentCount
  }

  def collectionFrequency(node: Node,
    index: DiskIndex,
    lengths: ARCA): Double = {
    collectionCount(node, index) / collectionLength(lengths)
  }

  def documentCount(node: Node, index: Index) : Double = {
    val it = index.getIterator(node)
    it match {
      case _: NullExtentIterator => 0.5
      case _ => {
        val stats = it.asInstanceOf[ARNA].getStatistics()
        stats.nodeDocumentCount.toDouble
      }
    }
  }

  def bowNodes(query: String) : List[Node] = {
    val queryTerms = """\w+""".r.findAllIn(query).toList
    queryTerms.map { T => Term(T) }
  }

  def unigrams(
    nodeMap: Map[Node, TEI],
    index: DiskIndex,
    lengths : ARCA
  ) : List[ParameterizedScorer] = {
    nodeMap.keys.map { Q =>
      val scorer = dirichlet(collectionFrequency(Q, index, lengths))
      ParameterizedScorer(scorer, lengths, nodeMap(Q))
    }.toList
  }

  def orderedWindows(
    nodeMap: Map[Node, TEI],
    index: DiskIndex,
    lengths: ARCA,
    width: Int = 1
  ) : List[ParameterizedScorer] = {
    nodeMap.keys.toList.sliding(2,1).map { termPair =>
      val scorer = dirichlet(collectionFrequency(termPair(0), index, lengths))
      val isect = od(width)
      val iterators = termPair.map(nodeMap(_))
      ParameterizedScorer(scorer, isect, lengths, iterators)
    }.toList
  }

  def unorderedWindows(
    nodeMap: Map[Node, TEI],
    index: DiskIndex,
    lengths: ARCA,
    width: Int = 1
  ) : List[ParameterizedScorer] = {
    nodeMap.keys.toList.sliding(2,1).map { termPair =>
      val scorer = dirichlet(collectionFrequency(termPair(0), index, lengths))
      val isect = uw(width)
      val iterators = termPair.map(nodeMap(_))
      ParameterizedScorer(scorer, isect, lengths, iterators)
    }.toList
  }
}
