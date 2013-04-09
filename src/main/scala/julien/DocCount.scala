package julien

import org.lemurproject.galago.tupleflow.Parameters
import java.io.PrintStream

object DocCount extends CLIFunction {
  def name = "doccount"
  def help = """doccount --x="<countable-query>" --index=<index directory>
Returns the number of documents that contain the countable-query.
More than one index and expression can be specified.
Examples of countable-exps: terms, ordered windows and unordered windows."""

  def run(p: Parameters, out: PrintStream) : Unit = {

  }
}
