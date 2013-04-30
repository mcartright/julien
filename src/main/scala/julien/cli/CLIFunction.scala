package julien

import julien.galago.tupleflow.Parameters
import java.io.PrintStream

/** Behavior needed for a valid CLI-interfaced class. */
trait CLIFunction {

  /** Command-line name of the function. Sort of a short "toString". */
  def name: String

  /** Helpful blurb on what parameters the function needs. */
  def help: String

  /** Returns true if the CLI function will execute correctly
    * given the parameters.
    */
  def checksOut(p: Parameters): Boolean

  /** Actually runs the function given the parameters.
    */
  def run(p: Parameters, out: PrintStream) : Unit

}
