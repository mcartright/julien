package julien
package learning
package features

trait Normalizer {
  def name: String
  def normalize(rl: RankList): Unit
  def normalize(rl: Ranklist, fids: Array[Int]): Unit
  def normalize(rls: List[RankList]) = rls foreach normalize
  def normalize(rls: List[RankList], fids: Array[Int]) =
    for (rl <- rls) normalize(rl, fids)
}
