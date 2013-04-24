package julien
package cli
package examples

import java.io.PrintStream

object MultiIndex extends Example {
  lazy val name: String = "multi"

  // This check needs improving
  // TODO: Make the parameters for this be case classes
  def checksOut(p: Parameters): Boolean = p.containsKey("query")

  val help: String = """
Is an example of a query that makes use of more than 1 index at a time.
"""

  def run(params: Parameters, out: PrintStream) {
    val terms = params.getString("query").split(" ")

    //
  }
}
