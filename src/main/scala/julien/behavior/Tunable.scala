package julien
package behavior

import scala.collection.Seq
import scala.math.Numeric

trait Tunable {
  def knobs: Seq[Knob[_]]
}

trait Knob[T] { outerKnob =>
  def name: String
  def step_=(v: T): Unit
  def step: T
  def max: T
  def min: T
  var value: T
  protected def getNumeric[A](implicit n: Numeric[A]) = n
  protected val numeric: Numeric[T]
  def minimize: Unit = value = this.min
  def maximize: Unit = value = this.max

  def turnUp: Boolean = {
    val newVal: T = numeric.plus(this.value, this.step)
    if (numeric.lt(newVal, this.max)) {
      value = newVal
      true
    } else {
      value = max
      false
    }
  }

  def turnDown: Boolean = {
    val newVal: T = numeric.minus(this.value, this.step)
    if (numeric.gt(newVal, this.max)) {
      value = newVal
      true
    } else {
      value = min
      false
    }
  }

  def values: Iterator[T] = new Iterator[T] {
    outerKnob.minimize
    def hasNext: Boolean = outerKnob.numeric.lt(outerKnob.value, outerKnob.max)
    def next: T = {
      val returnValue = outerKnob.value
      outerKnob.turnUp
      returnValue
    }
  }
}

abstract class DoubleKnob(val min: Double, val max: Double)
    extends Knob[Double]
{
  protected val numeric = getNumeric[Double]
  var step = (max - min) / (100)
}

abstract class IntKnob(val min: Int, val max: Int)
    extends Knob[Int]
{
  protected val numeric = getNumeric[Int]
  var step = 1 // Would like something better for this.
}
