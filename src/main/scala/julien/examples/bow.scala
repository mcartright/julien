package julien
package examples

import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConversions._
import org.lemurproject.galago.tupleflow.Parameters

import julien.retrieval._

/** Simple example showing how to compute a bag-of-words
  * query using Dirichlet smoothing.
  */
object BagOfWords extends Example {
  lazy val help: String = "Under construction"

  def run(args: Array[String]): Boolean = {
    val params = new Parameters(args)
    val query = params.getString("query").split(" ").map(Term(_))
    val ql = Combine(query.map(a => Dirichlet(a, LengthsView())): _*)

    // Open a small in-memory index
    val index : Index = Index.memory(params.getString("indexFiles"))

    // Make a processor to run it
    val processor = SimpleProcessor()

    // Attach the query model to the index
    ql.hooks.foreach(_.attach(index))

    // Add the model to the processor
    processor.add(ql)

    // run it and get results
    val results = processor.run

    // At some point we should verify correctness or print or something.
    return true
  }
}
