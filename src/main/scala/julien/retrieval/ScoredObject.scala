package julien

import scala.math.Ordered

trait ScoredObject {
  def id : InternalId
  def rank_=(i: Int): Unit
  def rank: Int
  def name_=(s: String): Unit
  def name: String
  def score: Double
}
