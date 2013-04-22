package julien
package learning
package boosting

class WeakRanker(val fid: Int, var weight: Double = 1.0) {
  def rank(rl: RankList): RankList = {
    val scores = rl.points.map(p => p(fid))
    val idx = Sorter.sort(scores, false)
    new RankList(rl, idx)
  }

  def rank(rls: List[RankList]): List[RankList] = rls.map(rl => rank(rl))
}
