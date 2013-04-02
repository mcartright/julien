package edu.umass.ciir.julien

class Dirichlet(
  bs: BoundSource,
  cs: CollectionSource,
  mu: Double = 1500) extends Feature {
  val cf = bs.collectionCount.toDouble / cs.collectionLength.toDouble

  def calculate : Double = {
    val num = bs.count + (mu*cf)
    val den = bs.length + mu
    scala.math.log(num / den)
  }
}
