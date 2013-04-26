package julien

import scala.math.Ordered

trait ScoredObject[T <: ScoredObject[T]] extends Ordered[T] {
  def compare(that: T): Int
}
