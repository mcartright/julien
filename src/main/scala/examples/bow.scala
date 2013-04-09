import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConversions._

object BOW extends App {
  val graph = QueryGraph()
  graph.addIndex('aquaint)

  // Need to simplify this
  val index = Sources.get('aquaint)
  graph.add("""\w+""".r.findAllIn(args(0)).toList.map { t =>
    Term(t, dirichlet(collectionFrequency(t, index)))
  }


  val executor = ExecutionGraph(graph)
  val resultQueue = executor.run

  // Get doc names and print
  printResults(resultQueue)
}
