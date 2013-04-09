package edu.umass.ciir.julien

object SingleTermView { def apply(t: Term) = new SingleTermView(t) }

class SingleTermView(val t: Term)
    extends PositionsView {
  lazy val children: Seq[Operator] = List[Operator](t)
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
