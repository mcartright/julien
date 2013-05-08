package julien
package retrieval

abstract class MultiTermView(terms: Seq[PositionStatsView])
    extends PositionStatsView
    with Bounded
    with NeedsPreparing {
  // Make sure we're not making a single view of multiple indexes - that's weird
  lazy val verified =
    hooks.forall { t => t.attachedIndex == hooks.head.attachedIndex }

  def children: Seq[Operator] = terms
  def count: Int = this.positions.position

  override lazy val isDense: Boolean = {
    val ops =
      terms.filter(_.isInstanceOf[Operator]).map(_.asInstanceOf[Operator])
    val movers = ops.flatMap(_.movers)
    movers.exists(_.isDense)
  }

  override def size: Int = statistics.docFreq.toInt

  // Start with no knowledge
  val statistics = CountStatistics()

  def updateStatistics(docid: InternalId) = {
    assume(verified, s"Tried to use multi-term view from different indexes.")
    val c = count
    statistics.collFreq += c
    statistics.docFreq += 1
    statistics.max = scala.math.min(statistics.max, c)
    statistics.numDocs = terms.head.statistics.numDocs
  }

}
