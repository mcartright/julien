package julien

import scala.math.Ordered

trait ScoredObject[T <: ScoredObject[T]]
    extends Comparable[T]
    with Ordered[T] {
  def id : InternalId
  def rank_=(i: Int): Unit
  def rank: Int
  def name: String
  def score: Double
}
