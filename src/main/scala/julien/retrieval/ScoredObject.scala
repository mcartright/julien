package julien

import scala.math.Ordered

trait ScoredObject[T <: ScoredObject[T]] extends Ordered[T] {
  def rank_=(i: Int): Unit
  def rank: Int
  def name: String
  def score: Double
  def compare(that: T): Int
}
