package julien
package cli

import java.io.{PrintStream, File}

object DocCount extends CLIFunction {
  def name = "doccount"

  def checksOut(p: Parameters): Boolean =
    (p.containsKey("x") &&
      p.containsKey("index") &&
      new File(p.getString("index")).isDirectory
    )


  def help = """
  Returns the number of documents that contain the countable-query.
  More than one index and expression can be specified.
  Examples of countable-exps: terms, ordered windows and unordered windows.

Required Parameters

x              An expression that is parsable into a Julien-style view.
index          The directory of the index to scan.
"""

  def run(p: Parameters, out: PrintStream) : Unit = {
    throw new UnsupportedOperationException("To be implemented")
  }
}
