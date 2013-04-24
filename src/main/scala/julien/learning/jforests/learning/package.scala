package julien.learning
package jforests

package object learning {
  val MIN_EXP_POWER = -50
  val EPSILON = 1.4E-45
  val LN2 = scala.math.log(2)
  val WIGGLE = 1E-6

  // To extend Double for approx compares
  implicit class ApproxDouble(d: Double) extends Ordered[Double] {
    def ~(other: Double): Boolean = (d - other < WIGGLE) && (other-d < WIGGLE)
  }
}
