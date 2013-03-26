import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConversions._

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
