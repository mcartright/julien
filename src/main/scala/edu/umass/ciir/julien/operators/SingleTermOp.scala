package operators

object SingleTermOp {
  def apply(t: Term) = new SingleTermOp(t)
}

class SingleTermOp(val t: Term)
    extends PositionsOp {
  import edu.umass.ciir.julien.Aliases._
  def count: Count = new Count(t.underlying.count)
  def positions: Positions = Positions(t.underlying.extents())
  lazy val statistics: CountStatistics = {
    val ns = t.underlying.asInstanceOf[ARNA].getStatistics
    val cs = t.attachedIndex.lengthsIterator.asInstanceOf[ARCA].getStatistics
    CountStatistics(
      new CollFreq(ns.nodeFrequency),
      new NumDocs(cs.documentCount),
      new CollLength(cs.collectionLength),
      new DocFreq(ns.nodeDocumentCount),
      new MaximumCount(ns.maximumCount.toInt)
    )
  }
}
