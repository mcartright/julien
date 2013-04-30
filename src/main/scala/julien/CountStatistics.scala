package julien

/** A mix of collection-level and view-specific statistics.
  * This is a case class to provide easy copy semantics.
  */
case class CountStatistics(
  var collFreq: Long = 0L,
  var numDocs: Long = 0L,
  var collLength: Long = 0L,
  var docFreq: Long = 0L,
  var max: Int = 0,
  var longestDoc: Int = 0
)
