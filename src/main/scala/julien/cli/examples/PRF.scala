package julien
package cli
package examples

import julien.retrieval._
import julien.retrieval.Utils._
import julien.retrieval.processor._
import julien.eval.QueryResult

import julien.galago.tupleflow.Parameters
import scala.collection.JavaConversions._
import scala.collection.immutable.Set
import scala.collection.Map

import java.io.PrintStream

object PRF extends Example {
  lazy val name: String = "relmodel"

  def checksOut(p: Parameters): Boolean =
    (p.containsKey("query") && p.containsKey("index"))

  val help: String = """
Executes the RM3 variant of the Relevance Model.
Required parameters:

    query        string form of desired query
    index        location existing index
"""



  def run(params: Parameters, out: PrintStream): Unit = {
    // Open an index - the implicit is to have it auto set for anything
    // that needs it.
    implicit val index : Index = Index.disk(params.getString("index"))

    // Set up to perform the first run
    val query = params.getString("query").split(" ").map(Term(_))
    val ql = Combine(query.map(a => Dirichlet(a, IndexLengths())))

    // run it and get results for the first run
    val results = QueryProcessor(ql)
    val selectedGrams = RelevanceModel(results, index, 10, 20)

    // Prep for adding to model
    val wrappedGrams = selectedGrams.map { gram =>
      Dirichlet(Term(gram.term), IndexLengths(), gram.score)
    }

    // Set weights using a single parameter (which ties them properly)
    val lambda = 0.7
    ql.weight = lambda
    val rm3 = Combine(
      List(ql, Combine(children = wrappedGrams, weight = (1-lambda)))
    )

    val finalResults = QueryProcessor(rm3)
    printResults(results, index, out)
  }
}
