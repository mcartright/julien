package edu.umass.ciir.julien.execution

import scala.collection.mutable.PriorityQueue

import org.lemurproject.galago.core.index.{Index, AggregateReader}
import org.lemurproject.galago.core.retrieval.iterator.MovableLengthsIterator
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator
import org.lemurproject.galago.core.retrieval.iterator.ExtentArrayIterator
import org.lemurproject.galago.core.retrieval.iterator.NullExtentIterator
import org.lemurproject.galago.core.retrieval.iterator.MovableExtentIterator
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator
import org.lemurproject.galago.core.util.ExtentArray
import org.lemurproject.galago.core.retrieval.processing.ScoringContext

case class ScoredDocument(docid: Int, score: Double)
object ScoredDocumentOrdering extends Ordering[ScoredDocument] {
  def compare(a: ScoredDocument, b: ScoredDocument) = b.score compare a.score
}

class ExecutionGraph(graph: QueryGraph) {
  type ARCA = AggregateReader.CollectionAggregateIterator
  type ARNA = AggregateReader.NodeAggregateIterator
  type TEI = MovableExtentIterator
  type TCI = MovableCountIterator
  type MLI = MovableLengthsIterator
  type CountScorer = (Int, Int) => Double
  type FeatureFunction = () => Double
  type Intersection = (List[ExtentArray]) => Int

  implicit def lengths2ARCA(l: MovableLengthsIterator) = l.asInstanceOf[ARCA]
  implicit def ARCA2lengths(l: ARCA) = l.asInstanceOf[MovableLengthsIterator]
  implicit def obj2countIt(o: java.lang.Object) = o.asInstanceOf[TCI]
  implicit def obj2extentIt(o: java.lang.Object) = o.asInstanceOf[TEI]
  implicit def obj2movableIt(o: java.lang.Object) =
    o.asInstanceOf[MovableIterator]

  // prepare the needed components to run
  val index = Sources.get(graph.defaultIndex)
  val lengths = index.getLengthsIterator
  val nodeMap = LinkedHashMap[Node, java.lang.Object]()
  for (n <- graph.leaves) {
    nodeMap.update(n,
      index.getIterator(n).asInstanceOf[PositionIndexReader#TermExtentIterator])
  }
  val iterators = nodeMap.values.toList.map(obj2moveableIt(_))
  val scorers = graph.scoreNodes.map(_.scorer)

  def run(numResults: Int = 100) : PriorityQueue[ScoredDocument] = {
    val ctx = new ScoringContext()
    // set contexts
    lengths.setContext(ctx)
    iterators.foreach(_.setContext(ctx))

    val resultQueue = PriorityQueue[ScoredDocument]()(ScoredDocumentOrdering)
    while (iterators.exists(_.isDone == false)) {
      val candidate = iterators.filterNot(_.isDone).map(_.currentCandidate).min
      ctx.document = candidate
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

  def formatNode(
    term: String,
    nodeType:String = "counts",
    field: String = null) : Node = {
    val input = field match {
      case null => String.format("#%s:%s:part=postings()", nodeType, term)
      case _ => String.format("#%s:%s:part=field.%s()", nodeType, term, field)
    }
    StructuredQuery.parse(input)
  }
}
