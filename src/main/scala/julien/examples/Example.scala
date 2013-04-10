package julien
package examples

/** Interface expected for anything implementing an example
  */
trait Example {

  /** Informative blurb indicating what is needed for the example to run. */
  def help: String

  /** Should return true if the example ran correctly. False otherwise. */
  def run(args: Array[String]): Boolean
}
