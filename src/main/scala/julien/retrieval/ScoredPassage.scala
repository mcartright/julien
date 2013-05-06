package julien
package retrieval

 /**
   * A specialized tuple for holding subsections ("slices")
   * of a document. Encapsulates a simple [begin, end] range
   * in the document. Only correctness check is that begin < end.
   *
   * Natural ordering is assumed to be by score.
   */
case class ScoredPassage(
  val id: InternalId,
  var score: Double,
  val begin: Int,
  val end: Int,
  var name: String  = "unknown",
  var rank: Int = 0) extends ScoredObject[ScoredPassage] {
  assume(begin < end, s"Can't have a scored passage with bad indices.")

  /** Compares passages by score. */
  def compare(that: ScoredPassage): Int =
    if (that.score < this.score) return -1
    else if (that.score > this.score) return 1
    else (this.id.underlying - that.id.underlying)

  override def compareTo(that: ScoredPassage): Int =
    if (that.score < this.score) return -1
    else if (that.score > this.score) return 1
    else (this.id.underlying - that.id.underlying)

  def +=(scoreToAdd: Double): Unit = score += scoreToAdd
  def -=(scoreToSubtract: Double): Unit = score -= scoreToSubtract
}
