package julien
package retrieval

import julien.behavior._

object IDF { def apply(s: StatisticsView) = new IDF(s) }

/** Just an Inverse Document Frequency (IDF) feature. Calculates
  *  the feature once (lazily) then caches it.
  */
class IDF(val statsrc: StatisticsView)
    extends ScalarWeightedFeature
    with Bounded
{
  lazy val children: Array[Operator] = Array[Operator](statsrc)
  lazy val views: Set[View] = Set[View](statsrc)
  // Runs when asked for the first time, and runs only once
  lazy val idf = {
    val stats: CountStatistics = statsrc.statistics
    scala.math.log(stats.numDocs / (stats.docFreq + 0.5))
  }

  override lazy val upperBound: Double = idf
  override lazy val lowerBound: Double = idf
  lazy val score: Double = idf
  // Static value
  def eval(id: InternalId): Double = score
}
