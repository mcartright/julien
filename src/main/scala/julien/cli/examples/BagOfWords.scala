package julien
package cli
package examples

import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConversions._
import julien.galago.tupleflow.Parameters
import java.io.PrintStream

import julien.retrieval._
import julien.retrieval.Utils._

/** Simple example showing how to compute a bag-of-words
  * query using Dirichlet smoothing.
  */
object BagOfWords extends Example {
  lazy val name: String = "bow"

  def checksOut(p: Parameters): Boolean =
    (p.containsKey("query") && p.containsKey("index"))

  val help: String = """
Execute a query as a bag of words.
Required parameters:

    query        string form of desired query
    index        location of an existing index
"""

  def run(params: Parameters, out: PrintStream): Unit = {
    val query = params.getString("query").split(" ").map(Term(_))
    val ql = Combine(query.map(a => Dirichlet(a, IndexLengths())))

    // Open a small in-memory index
    val index : Index = Index.disk(params.getString("index"))

    // Make a processor to run it
    val processor = MaxscoreProcessor()

    // Attach the query model to the index
    ql.hooks.foreach(_.attach(index))

    // Add the model to the processor
    processor.add(ql)

    // run it and get results
    val results = processor.run()
    printResults(results, index, out)
  }
}
