package julien
package retrieval

abstract class MultiTermView(terms: Seq[PositionStatsView])
    extends PositionStatsView
    with NeedsPreparing {
  // Make sure we're not making a single view of multiple indexes - that's weird
  lazy val verified =
    hooks.forall { t => t.attachedIndex == hooks.head.attachedIndex }

  def children: Seq[Operator] = terms
  def count: Count = new Count(this.positions.size)

  // Start with no knowledge
  val statistics = CountStatistics()

  def updateStatistics = {
    assume(verified, s"Tried to use multi-term view from different indexes.")
    val c = count.underlying
    statistics.collFreq += c
    statistics.docFreq += 1
    statistics.max = scala.math.min(statistics.max.underlying, c)
    statistics.numDocs = terms.head.statistics.numDocs
  }
}
