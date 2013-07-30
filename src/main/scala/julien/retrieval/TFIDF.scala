package julien
package retrieval

import julien.behavior._

object TFIDF {
  def apply(op: CountStatsView, l: LengthsView): TFIDF = apply(op, l, op)
  def apply(c: CountView, l: LengthsView, s: StatisticsView) =
    if (c.isInstanceOf[Movable])
      new TFIDF(c, l, s) with Driven { val driver = c.asInstanceOf[Movable] }
    else
      new TFIDF(c, l, s)
}

/** Term Frequency (TF) * Inverse Document Frequency (IDF). Classic term-level
  * scoring function. Useful as a feature in larger models, and pedagogically.
  */
class TFIDF(
  val op: CountView,
  val lengths: LengthsView,
  val statsrc: StatisticsView)
    extends ScalarWeightedFeature
    with Bounded
{

  lazy val children: Array[Operator] =
    Set[Operator](op, lengths, statsrc).toArray
  lazy val views: Set[View] = Set[View](op, lengths, statsrc)
  // Runs when asked for the first time, and runs only once
  lazy val idf = {
    val stats: CountStatistics = statsrc.statistics
    scala.math.log(stats.numDocs / (stats.docFreq + 0.5))
  }

  override lazy val upperBound: Double = {
    val maxtf = statsrc.statistics.max
    score(maxtf, maxtf)
  }

  override lazy val lowerBound: Double = 0.0
  def eval(id: InternalId): Double = score(op.count(id), lengths.length(id))
  def score(c: Int, l: Int): Double = (c.toDouble / l.toDouble) * idf
}
