package julien
package retrieval

import julien.behavior._

object BM25 {
  private val defB = 0.75
  private val defK = 1.2

  def apply(
    op: PositionStatsView,
    l: LengthsView
  ): BM25 = apply(op, l, op)

  def apply(
    op: PositionStatsView,
    l: LengthsView,
    b: Double
  ): BM25 = apply(op, l, op, b = b)

  def apply(
    op: PositionStatsView,
    l: LengthsView,
    b: Double,
    k: Double
  ): BM25 = apply(op, l, op, b, k)

  def apply(
    c: CountView,
    l: LengthsView,
    s: StatisticsView,
    b: Double = defB,
    k: Double = defK
  ) = if (c.isInstanceOf[Movable])
    new BM25(c, l, s, b, k) with Driven { val driver = c.asInstanceOf[Movable] }
  else
    new BM25(c, l, s, b, k)
}

/** Smoothes raw counts according to the BM25 scoring model, as described by
  * "Experimentation as a way of life: Okapi at TREC" by
  * Robertson, Walker, and Beaulieu.
  * (http://www.sciencedirect.com/science/article/pii/S0306457399000461)
  */
class BM25(
  val op: CountView,
  val lengths: LengthsView,
  val statsrc: StatisticsView,
  val b: Double,
  val k: Double)
    extends ScalarWeightedFeature
    with Bounded
{
  require(b > 0.0 && b < 1.0, s"b must be in [0,1]. Got $b")
  require(k > 0.0, s"k must be positive. Got $k")
  lazy val children: Seq[Operator] = Set[Operator](op, lengths, statsrc).toList
  lazy val views: Set[View] = Set[View](op, lengths, statsrc)

  // Runs when asked for the first time, and runs only once
  lazy val stats = statsrc.statistics
  lazy val avgDocLength = stats.collLength.toDouble / stats.numDocs
  lazy val idf = scala.math.log(stats.numDocs / (stats.docFreq + 0.5))

  // Yay - plays nice w/ the bounds.
  override val lowerBound: Double = 0.0
  override lazy val upperBound: Double = {
    val maxtf = statsrc.statistics.max
    score(maxtf, maxtf)
  }

  def eval(id: InternalId): Double = score(op.count(id), lengths.length(id))

  def score(c: Int, l: Int) = {
    val num = c * (k + 1)
    val den = c + (k * (1 - b + (b * l / avgDocLength)))
    idf * num / den
  }
}

