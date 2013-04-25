package julien
package retrieval

object TFIDF {
  def apply(op: PositionStatsView, l: LengthsView) = {
    new TFIDF(op, l, op)
  }
  def apply(c: CountView, l: LengthsView, s: StatisticsView) =
    new TFIDF(c, l, s)
}

/** Term Frequency (TF) * Inverse Document Frequency (IDF). Classic term-level
  * scoring function. Useful as a feature in larger models, and pedagogically.
  */
class TFIDF(
  val op: CountView,
  val lengths: LengthsView,
  val statsrc: StatisticsView)
    extends FeatureOp {

  lazy val children: Seq[Operator] = Set[Operator](op, lengths, statsrc).toList
  lazy val views: Set[ViewOp] = Set[ViewOp](op, lengths, statsrc)
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
  def eval: Double = score(op.count, lengths.length)
  def score(c: Int, l: Int): Double = (c.toDouble / l.toDouble) * idf
}
