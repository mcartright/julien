import org.lemurproject.galago.core.index.disk.DiskIndex
import org.lemurproject.galago.core.index.disk.PositionIndexReader
import org.lemurproject.galago.core.retrieval.query.{StructuredQuery, Node}
import org.lemurproject.galago.core.parse.Document

import org.lemurproject.galago.tupleflow.Parameters
import scala.collection.JavaConversions._
import scala.collection.Map

object GalagoBridging {


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

// Extract terms we want to use for expansion
case class Gram(term: String, score: Double)
object GramOrdering extends Ordering[Gram] {
  def compare(a: Gram, b: Gram) = {
    val result = b.score compare a.score
    if (result == 0) a.term compare b.term else result
  }
}
