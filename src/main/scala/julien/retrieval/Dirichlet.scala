package julien
package retrieval

import julien.behavior._

object Dirichlet {
  val defaultMu = 1500D
  val totallyMadeUpValue = 600

  def apply(op: CountStatsView, l: LengthsView): Dirichlet = apply(op, l, op)
  def apply(op: CountStatsView,
    l: LengthsView,
    mu: Double
  ): Dirichlet = apply(op, l, op, mu = mu)

  def apply(op: CountStatsView,
    l: LengthsView,
    mu: Double,
    weight: Double
  ): Dirichlet = apply(op, l, op, mu, weight)

  def apply(
    op: CountView,
    l: LengthsView,
    s: StatisticsView,
    mu: Double = defaultMu,
    weight: Double = 1.0
  ): Dirichlet = if (op.isInstanceOf[Movable])
    new Dirichlet(op, l, s, mu, () => weight) with Driven {
      val driver = op.asInstanceOf[Movable]
    }
  else
    new Dirichlet(op, l, s, mu, () => weight)
}

class Dirichlet(
  val op: CountView,
  val lengths: LengthsView,
  val statsrc: StatisticsView,
  val mu: Double,
  w: () => Double)
    extends ScalarWeightedFeature
    with Bounded
{
  require(mu > 0, s"Mu must be positive. Received $mu")
  this.weight = w()
  override def terse: String = s"Dir(${op.terse})"
  lazy val children: Array[Operator] =
    Set[Operator](op, lengths, statsrc).toArray
  lazy val views: Set[View] = Set[View](op, lengths, statsrc)

  // Runs when asked for the first time, and runs only once
  lazy val cf = {
    val stats: CountStatistics = statsrc.statistics
    val cf = if (stats.collFreq == 0) {
      0.5 / stats.collLength
    } else {
      stats.collFreq.toDouble / stats.collLength
    }
    cf
  }

  override lazy val upperBound: Double = {
    val maxtf = statsrc.statistics.max
    score(maxtf, maxtf)
  }

  override lazy val lowerBound: Double = {
    // Not sure which one is less accurate, actually...
    //score(0, statsrc.statistics.longestDoc)
    score(0, Dirichlet.totallyMadeUpValue)
  }

  def eval(id: Int): Double = score(op.count(id), lengths.length(id))

  def score(c: Int, l: Int): Double = {
    val num = (c + (mu * cf))
    val den = (l + mu)
    val rawScore = scala.math.log((c + (mu * cf)) / (l + mu))
    scala.math.log((c + (mu * cf)) / (l + mu))
  }
}
