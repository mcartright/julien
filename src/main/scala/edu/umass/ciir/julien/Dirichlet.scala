package edu.umass.ciir.julien

class Dirichlet(src: KeyedSource, mu: Double = 1500) extends Feature {
  val cf = src.collectionCount.toDouble / src.collectionLength.toDouble

  def calculate : Double = {
    val num = src.count + (mu*cf)
    val den = src.length + mu
    scala.math.log(num / den)
  }
}
