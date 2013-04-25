package julien
package retrieval

object IDF { def apply(s: StatisticsView) = new IDF(s) }

/** Just an Inverse Document Frequency (IDF) feature. Calculates
  *  the feature once (lazily) then caches it.
  */
class IDF(val statsrc: StatisticsView) extends FeatureOp {
  lazy val children: Seq[Operator] = List[Operator](statsrc)
  lazy val views: Set[ViewOp] = Set[ViewOp](statsrc)
  // Runs when asked for the first time, and runs only once
  lazy val idf = {
    val stats: CountStatistics = statsrc.statistics
    scala.math.log(stats.numDocs / (stats.docFreq + 0.5))
  }

  override lazy val upperBound: Double = idf
  override lazy val lowerBound: Double = idf
  lazy val score: Double = idf
  val eval: Double = score
}
