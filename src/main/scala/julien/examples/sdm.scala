package julien
package examples

import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConversions._
import org.lemurproject.galago.tupleflow.Parameters

// Pull in retrieval definitions
import julien.retrieval._

object SDM extends Example {
  lazy val help: String = "Under construction"

  def run(args: Array[String]): Boolean = {
    val params = new Parameters(args)
    val query = params.getString("query").split(" ").map(Term(_))
    val sdm =
      Combine(
        Weight(Combine(query.map(a => Dirichlet(a,LengthsView())): _*), 0.8),
        Weight(Combine(query.sliding(2,1).map { p =>
          Dirichlet(OrderedWindow(1, p: _*), LengthsView())
        }.toSeq: _*), 0.15),
        Weight(Combine(query.sliding(2,1).map { p =>
          Dirichlet(UnorderedWindow(8, p: _*), LengthsView())
        }.toSeq: _*), 0.05)
      )

    // Open a small in-memory index
    val index : Index = Index.memory(params.getString("indexFiles"))

    // Make a processor to run it
    val processor = SimpleProcessor()

    // Attach the query model to the index
    sdm.hooks.foreach(_.attach(index))

    // Add the model to the processor
    processor.add(sdm)

    // run it and get results
    val results = processor.run

    // At some point we should verify correctness or print or something.
    return true
  }
}
