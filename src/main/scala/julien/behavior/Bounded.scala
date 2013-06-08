package julien
package behavior

/** This trait indicates that the operator can provide upper and
  * lower bounds for its score range. This provides important information
  * for pruning algorithms.
  */
trait Bounded {
  /** The highest possible score that can be produced by this Feature.
    * Need not be a tight bound, but the tighter the better.
    */
  def upperBound: Double

  /** The lowest possible score that can be produce by this Feature.
    * Need not be a tight bound, but the tighter the better.
    */
  def lowerBound: Double
}
