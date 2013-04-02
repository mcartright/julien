package edu.umass.ciir.julien

abstract class MultiTermOp(terms: Seq[Term]) extends PositionsOp {
  // Lazily verifies that all terms
  lazy val verified = {
    val r = terms.forall { t => t.attachedIndex == terms.head.attachedIndex }
    assume(r, s"Tried to use multi-term op from different indexes.")
    true // If we made it here, must be true
  }
  def children: Seq[Operator] = terms
  def count: Count = new Count(this.positions.size)

  lazy val statistics: CountStatistics = {
    assume(verified) // precondition which means we can use any of them
    val index = terms.head.attachedIndex
    val stats = CountStatistics(
      new CollFreq(0),
      new NumDocs(0),
      new CollLength(0),
      new DocFreq(0),
      new MaximumCount(0)
    )

    // Need to fill in values here I think

    stats
  }
}
