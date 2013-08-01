package julien

trait ScoredObject {
  def id : Int
  def rank_=(i: Int): Unit
  def rank: Int
  def name_=(s: String): Unit
  def name: String
  def score: Double
}
