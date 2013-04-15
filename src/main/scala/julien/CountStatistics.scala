package julien

/** A mix of collection-level and view-specific statistics. */
object CountStatistics {
  def apply() = new CountStatistics(
      new CollFreq(0),
      new NumDocs(0),
      new CollLength(0),
      new DocFreq(0),
      new MaximumCount(0))

  def apply(
    cf: CollFreq,
    nd: NumDocs,
    cl: CollLength,
    df: DocFreq,
    mc: MaximumCount
  ) = new CountStatistics(cf, nd, cl, df, mc)
}

class CountStatistics(
  var collFreq: CollFreq,
  var numDocs: NumDocs,
  var collLength: CollLength,
  var docFreq: DocFreq,
  var max: MaximumCount
)
