package julien

import scala.math.Ordered

trait ScoredObject[T <: ScoredObject[T]] extends Ordered[T] {
  def id: String
  def score: Double
  def compare(that: T): Int
}
