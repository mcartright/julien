import org.lemurproject.galago.core.index.disk.DiskIndex
import org.lemurproject.galago.core.index.disk.PositionIndexReader
import org.lemurproject.galago.core.retrieval.query.{StructuredQuery, Node}
import org.lemurproject.galago.core.parse.Document
import org.lemurproject.galago.core.index.{Index, AggregateReader}
import org.lemurproject.galago.core.retrieval.iterator.MovableLengthsIterator
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator
import org.lemurproject.galago.core.retrieval.iterator.ExtentArrayIterator
import org.lemurproject.galago.core.retrieval.iterator.NullExtentIterator
import org.lemurproject.galago.core.retrieval.iterator.MovableExtentIterator
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator
import org.lemurproject.galago.core.util.ExtentArray
import org.lemurproject.galago.core.retrieval.processing.ScoringContext
import org.lemurproject.galago.tupleflow.Parameters
import scala.collection.JavaConversions._
import scala.collection.Map

import scala.collection.mutable.PriorityQueue

object GalagoBridging {
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

  // Intersection generators
  def od(width: Int = 1) : Intersection =
    (extents: List[ExtentArray]) => {
      val iterators = extents.map(new ExtentArrayIterator(_))
      var count = 0
      while (iterators.forall(_.isDone == false)) {
        // Make sure an iterator is after its preceding one
        iterators.reduceLeft { (i1, i2) =>
          while (i2.currentBegin < i1.currentEnd && !i2.isDone) i2.next
          i2
        }
        // Now see if we have a valid match
        val matched = iterators.sliding(2,1).map { P =>
          // Map pairs of iterators to booleans. All true = have match
          P(1).currentBegin - P(0).currentEnd < width
        }.reduceLeft((A,B) => A && B)
        if (matched) count += 1
        iterators(0).next
      }
      count
    }

  def uw(width: Int = 1) : Intersection =
    (extents: List[ExtentArray]) => {
      val iterators = extents.map(new ExtentArrayIterator(_))
      var count = 0
      while (iterators.forall(_.isDone == false)) {
        // Find bounds
        val minPos = iterators.map(_.currentBegin).min
        val maxPos = iterators.map(_.currentEnd).max

        // see if it fits
        if (maxPos - minPos <= width || width == -1) count += 1

        // move all lower bound iterators foward
        for (it <- iterators; if (it.currentBegin == minPos)) it.next
      }
      count
    }

  // Scoring function generators
  def dirichlet(cf: Double, mu: Double = 1500) : CountScorer =
    (count:Int, length: Int) => {
      val num = count + (mu*cf)
      val den = length + mu
      scala.math.log(num / den)
    }

  def jm(cf: Double, lambda: Double = 0.2) : CountScorer =
    (count:Int, length: Int) => {
      val foreground = count.toDouble / length
      scala.math.log((lambda*foreground) + ((1.0-lambda)*cf))
    }

  def bm25(
    adl: Double,
    idf: Double,
    b: Double = 0.75,
    k: Double = 1.2) : CountScorer =
    (count: Int, length: Int) => {
      val num = count * (k + 1)
      val den = count + (k * (1 - b + (b * length / adl)))
      idf * num / den
    }

  def printResults(
    results: List[ScoredDocument],
    index: Index) : Unit = {
    for ((sd, idx) <- results.zipWithIndex) {
      val name = index.getName(sd.docid)
      Console.printf("test %s %f %d julien\n",
        name, sd.score, idx+1)
    }
  }

  def printResults(
    results: PriorityQueue[ScoredDocument],
    index: Index) : Unit = {
    var rank = 1
    while (!results.isEmpty) {
      val doc = results.dequeue
      val name = index.getName(doc.docid)
      Console.printf("test %s %f %d julien\n",
        name, doc.score, rank)
      rank += 1
    }
  }

  def bowNodes(query: String, nodeType: String = "counts") : List[Node] = {
    val queryTerms = """\w+""".r.findAllIn(query).toList
    queryTerms.map { T => formatNode(T, nodeType) }
  }

  def unigrams(
    nodeMap: Map[Node, java.lang.Object],
    index: DiskIndex,
    lengths : ARCA
  ) : List[ParameterizedScorer] = {
    nodeMap.keys.map { Q =>
      val scorer = dirichlet(collectionFrequency(Q, index, lengths))
      ParameterizedScorer(scorer, lengths, nodeMap(Q))
    }.toList
  }

  def orderedWindows(
    nodeMap: Map[Node, java.lang.Object],
    index: DiskIndex,
    lengths: ARCA,
    width: Int = 1
  ) : List[ParameterizedScorer] = {
    nodeMap.keys.toList.sliding(2,1).map { termPair =>
      val scorer = dirichlet(collectionFrequency(termPair(0), index, lengths))
      val isect = od(width)
      val iterators =
        termPair.map(nodeMap(_)).map(_.asInstanceOf[GalagoBridging.TEI])
      ParameterizedScorer(scorer, isect, lengths, iterators)
  }.toList
}

  def unorderedWindows(
    nodeMap: Map[Node, java.lang.Object],
    index: DiskIndex,
    lengths: ARCA,
    width: Int = 1
  ) : List[ParameterizedScorer] = {
    nodeMap.keys.toList.sliding(2,1).map { termPair =>
      val scorer = dirichlet(collectionFrequency(termPair(0), index, lengths))
      val isect = uw(width)
      val iterators =
        termPair.map(nodeMap(_)).map(_.asInstanceOf[GalagoBridging.TEI])
      ParameterizedScorer(scorer, isect, lengths, iterators)
    }.toList
  }

 def standardScoringLoop(
   scorers: List[ParameterizedScorer],
   iterators: List[MovableIterator],
   lengths: MovableLengthsIterator,
   numResults: Int = 100
 ) : PriorityQueue[ScoredDocument] = {
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

  val digitPattern = """(\d+)""".r
  def isAllDigits(s: String) = s match {
    case digitPattern(n) => true
    case _ => false
  }

  def scrub(s: String) =
    s.filter(c => c.isLetterOrDigit || c.isWhitespace).toLowerCase

  def histogram(doc: Document) : Histogram = {
  // the case is to force Scala to make an identity function
    val h = doc.terms.groupBy { case s => s }.mapValues(_.size)
    Histogram(doc.identifier, h)
  }

  def multinomial(doc: Document) : Multinomial = {
    val h = doc.terms.groupBy { case s => s }.mapValues(_.size)
    val sum = h.values.sum
    val probs = h.mapValues(_.toDouble / sum)
    Multinomial(doc.identifier, probs)
  }
}

case class Histogram(id: Int, counts: Map[String, Int])
case class Multinomial(id: Int, probs: Map[String, Double])
case class ScoredDocument(docid: Int, score: Double)
object ScoredDocumentOrdering extends Ordering[ScoredDocument] {
  def compare(a: ScoredDocument, b: ScoredDocument) = b.score compare a.score
}

// Extract terms we want to use for expansion
case class Gram(term: String, score: Double)
object GramOrdering extends Ordering[Gram] {
  def compare(a: Gram, b: Gram) = {
    val result = b.score compare a.score
    if (result == 0) a.term compare b.term else result
  }
}
