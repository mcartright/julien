package julien
package cli
package examples

import scala.collection.JavaConversions._
import julien.galago.tupleflow.Parameters

// Pull in retrieval definitions
import julien.retrieval._
import julien.retrieval.Utils._
import julien.retrieval.processor._

import java.io.PrintStream

object SequentialDependenceModel extends Example {
  lazy val name: String = "sdm"

  def checksOut(p: Parameters): Boolean =
    (p.containsKey("query") && p.containsKey("index"))

  val help: String = """
Shows automatic expansion of a set of unigrams into the
Sequential Dependence Model, from
"A Markov random field model for term dependencies"
by Metzler and Croft, SIGIR 2005.

Required parameters:

    query        string form of desired query
    index        location of an existing index
"""

  def run(params: Parameters, out: PrintStream) {
    // Open an index
    implicit val index : Index = Index.disk(params.getString("index"))

    val query = params.getString("query").split(" ").map(Term(_))
    // Shorthand is:
    // val seqdep = sdm(query, Dirichlet.apply)
    val seqdep =
      Combine(List[Feature](
        Combine(children = query.map(a => Dirichlet(a,IndexLengths())),
          weight = 0.8),
        Combine(children = query.sliding(2,1).map { p =>
          Dirichlet(OrderedWindow(1, p: _*), IndexLengths())
        }.toSeq,
          weight = 0.15),
        Combine(query.sliding(2,1).map { p =>
          Dirichlet(UnorderedWindow(8, p: _*), IndexLengths())
        }.toSeq,
          weight = 0.05)
      ))

    // Make a processor to run it
    val processor = QueryProcessor(seqdep)

    // Use this to add a line-printing debugger - still experimental
    //processor.debugger =
    //  Some(julien.retrieval.LinePrintingDebugger().printState _)

    // run it and get results
    val results = processor.run()

    printResults(results, index, out)
  }
}
