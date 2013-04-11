package julien
package cli

import java.io.PrintStream
import org.slf4j.{Logger,LoggerFactory}
import scala.collection.JavaConversions._

object Julien {
  val log = LoggerFactory.getLogger("Julien")
  def main(argv: Array[String]) : Unit = run(argv)

  def run(argv: Array[String], out : PrintStream = Console.out) : Unit = {
    val names = findApps().map(a => (a.name, a)).toMap
    if (argv.size < 1 || argv.toSet("help")) {
      out.printf("Available functions:\n\n%s\n",
        names.keys.toList.sorted.mkString(","))
      return
    }
    val (fnName: String, remaining: Array[String]) = (argv.head, argv.tail)
    if (names.contains(fnName)) {
      names(fnName).run(remaining, out)
    } else {
      out.printf("Function '%s' was not found. Options: \n%s\n",
        fnName, names.keys.toList.sorted.mkString(","))
    }
  }

  // Would like to find these via reflection, but...not that easy right now.
  def findApps() : Iterable[CLIFunction] =
    List[CLIFunction](BuildIndex, DumpKeys, DocCount)
}
