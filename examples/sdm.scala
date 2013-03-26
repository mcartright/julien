import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf._
import edu.stanford.nlp.io.IOUtils
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.CoreAnnotations.AnswerAnnotation
import scala.collection.mutable.PriorityQueue
import org.lemurproject.galago.core.parse.{Document,Tag,TagTokenizer}
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.immutable.Set
import scala.collection.Map

import java.net.URL
import scala.io.Source._
import java.io.IOException

object SDM extends App {
  val graph = QueryGraph()
  graph.addIndex('aquaint)
  val queryTerms = """\w+""".r.findAllIn(args(0)).toList
  // "Explicit"
  graph.add(logsum(
    (unigrams(queryTerms), 0.8),
    (orderedWindows(queryTerms, width = 1), 0.15),
    (unorderedWindows(queryTerms, width = 8), 0.05)
  ))

  val executor = ExecutionGraph(graph)
  val resultQueue = executor.run

  // Get doc names and print
  printResults(resultQueue)
}
