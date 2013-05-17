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
) {
  def namedString: String = {
    val names = Seq("cf", "#docs", "collLen", "df", "maxcount", "longestDoc")
    val values = Seq(collFreq, numDocs, collLength, docFreq, max, longestDoc)
    names.zip(values).map(p => s"${p._1}=${p._2}").mkString("(",",",")")
  }
}
