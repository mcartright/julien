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

object BOW extends App {
  val graph = QueryGraph()
  graph.addIndex('aquaint)
  graph.add("""\w+""".r.findAllIn(args(0)).toList.map(t => Dirichlet(Term(t))))
  val executor = ExecutionGraph(graph)
  val resultQueue = executor.run

  // Get doc names and print
  printResults(resultQueue)
}
