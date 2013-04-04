package edu.umass.ciir.julien

import org.lemurproject.galago.tupleflow.Parameters
import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConversions._
import scala.util.matching.Regex

object Utils {
  class RichRegex(underlying: Regex) {
    def matches(s: String): Boolean = underlying.pattern.matcher(s).matches
    def misses(s: String): Boolean = (matches(s) == false)
  }

  implicit def regexToRichRegex(r: Regex) = new RichRegex(r)

  def printResults(results: List[ScoredDocument], index: Index) : Unit = {
    for ((sd, idx) <- results.zipWithIndex) {
      val name = index.name(sd.docid)
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
      val name = index.name(doc.docid)
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
}

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
