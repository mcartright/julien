package julien
package behavior

/** This trait indicates that the operator can provide upper and
  * lower bounds for its score range. This provides important information
  * for pruning algorithms.
  */
trait Bounded {
  def upperBound: Double
  def lowerBound: Double
}
