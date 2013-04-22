package julien
package learning

import scala.collection.mutable.ListBuffer
import julien.learning.util.Sorter

class RankList(input: List[DataPoint]) {
  val points = ListBuffer[DataPoint](input: _*)
  def this(other: RankList) = this(List(other.points: _*))
  def this(other: RankList, idx: List[Int]) =
    this(idx.map(i => other.points(i)))

  def add(p: DataPoint) = points += p
  def remove(i: Int) = points.remove(i)
  def size = points.size
  def apply(i: Int) = points(i)
  def get(i: Int) = points(i)
  def id = points(0).id
  def ranking(fid: Short): RankList = {
    val scores = points.map(_.features(fid))
    new RankList(this, Sorter.sort(scores, false))
  }

  def correctRanking: RankList = {
    val scores = points.map(_.label)
    new RankList(this, Sorter.sort(scores, false))
  }

  def worstRanking: RankList = {
    val scores = points.map(_.label)
    new RankList(this, Sorter.sort(scores, true))
  }
}
