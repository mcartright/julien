package edu.umass.ciir.julien

import org.lemurproject.galago.tupleflow.Parameters
import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConversions._
import scala.util.matching.Regex
import Aliases._

object Utils {
  class RichRegex(underlying: Regex) {
    def matches(s: String): Boolean = underlying.pattern.matcher(s).matches
    def misses(s: String): Boolean = (matches(s) == false)
  }

  implicit def regexToRichRegex(r: Regex) = new RichRegex(r)

  def printResults(results: List[ScoredDocument], index: GIndex) : Unit = {
    for ((sd, idx) <- results.zipWithIndex) {
      val name = index.getName(sd.docid)
      Console.printf("test %s %f %d julien\n",
        name, sd.score, idx+1)
    }
  }

  def printResults(
    results: PriorityQueue[ScoredDocument],
    index: GIndex) : Unit = {
    var rank = 1
    while (!results.isEmpty) {
      val doc = results.dequeue
      val name = index.getName(doc.docid)
      Console.printf("test %s %f %d julien\n",
        name, doc.score, rank)
      rank += 1
    }
  }

  val digitPattern = """(\d+)""".r
  def isAllDigits(s: String) = s match {
    case digitPattern(n) => true
    case _ => false
  }

  def scrub(s: String) =
    s.filter(c => c.isLetterOrDigit || c.isWhitespace).toLowerCase

  def histogram(doc: GDoc) : Histogram = {
    // the case is to force Scala to make an identity function
    val h = doc.terms.groupBy { case s => s }.mapValues(_.size)
    Histogram(doc.identifier, h)
  }

  def multinomial(doc: GDoc) : Multinomial = {
    val h = doc.terms.groupBy { case s => s }.mapValues(_.size)
    val sum = h.values.sum
    val probs = h.mapValues(_.toDouble / sum)
    Multinomial(doc.identifier, probs)
  }
}

case class ScoredDocument(docid: Int, score: Double)
object ScoredDocumentOrdering extends Ordering[ScoredDocument] {
  def compare(a: ScoredDocument, b: ScoredDocument) = b.score compare a.score
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
