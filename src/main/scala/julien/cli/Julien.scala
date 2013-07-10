package julien
package cli

import java.io.PrintStream
import org.slf4j.{Logger,LoggerFactory}
import scala.collection.JavaConversions._
import julien.cli.examples._

object Julien {
  val log = LoggerFactory.getLogger("Julien")

  /** For calling stuff from the command line.
    * Does all of the checking and printing out of
    * help and the like. Basically has you check yourself
    * before you wreck yourself.
    */
  def main(argv: Array[String]) : Unit = {
    val namesToFunctions = findFunctions.map(a => (a.name, a)).toMap
    if (argv.size < 1 ||
      argv.head == "help" ||
      !namesToFunctions.keys.contains(argv.head)) {
      printFunctions(namesToFunctions.values)
      return
    }

    val selectedFn: CLIFunction = namesToFunctions(argv.head)
    val parameters: Parameters = new Parameters(argv.tail)
    if (!selectedFn.checksOut(parameters)) {
      Console.print(s"""
Function ${selectedFn.name} did not receive proper parameters.

${selectedFn.help}
""")
      return
    }

    // Should be gtg here.
    selectedFn.run(parameters, Console.out)
  }

  /** Aliased to allow calling this function programmatically with an
    * alternative output stream. Also assumes you've already chosen a
    * function to execute. Will assume you checked out the parameters
    * first, so if it doesn't work correctly, you get an exception.
    */
  def run(
    fn: CLIFunction,
    p: Parameters,
    out : PrintStream = Console.out) {
    if (fn.checksOut(p)) {
      fn.run(p, out)
    } else {
      throw new IllegalArgumentException(
        f"Function ${fn.name} was not provided the right parameters."
      )
    }
  }

  /** Lists all available functions.
    * Assumes is being called from 'main', hence only prints information
    * out to the console.
    */
  def printFunctions(functions: Iterable[CLIFunction]) {
    // partition into just functions and examples
    val (examples, vanilla) = functions.partition(_.isInstanceOf[Example])
    // Show vanilla functions first
    val fnListing = vanilla.map(_.name).toList.sorted.mkString("\n\t")
    Console.println(s"Functions\n\n\t${fnListing}\n")
    // Now examples
    val exListing = examples.map(_.name).toList.sorted.mkString("\n\t")
    Console.println(s"Examples\n\n\t${exListing}\n")
  }

  // Would like to find these via reflection, but...not that easy right now.
  def findFunctions : Seq[CLIFunction] =
    // List of PO-functions
    List[CLIFunction](BuildIndex, DumpKeys, DocCount,
      // List of Examples
      BagOfWords, PRF, SequentialDependenceModel)
}
