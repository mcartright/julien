package edu.umass.ciir.julien

object TermScorers {
  type CountScorer = (Int, Int) => Double

  // Scoring function generators
  def dirichlet(cf: Double, mu: Double = 1500) : CountScorer =
    (count:Int, length: Int) => {
      val num = count + (mu*cf)
      val den = length + mu
      scala.math.log(num / den)
    }

  def jm(cf: Double, lambda: Double = 0.2) : CountScorer =
    (count:Int, length: Int) => {
      val foreground = count.toDouble / length
      scala.math.log((lambda*foreground) + ((1.0-lambda)*cf))
    }

  def bm25(
    adl: Double,
    idf: Double,
    b: Double = 0.75,
    k: Double = 1.2) : CountScorer =
    (count: Int, length: Int) => {
      val num = count * (k + 1)
      val den = count + (k * (1 - b + (b * length / adl)))
      idf * num / den
    }
}
