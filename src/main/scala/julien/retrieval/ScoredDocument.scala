package julien
package retrieval

/** A specialized Tuple for a scored document.
  * Holds the id of the document and the score.
  *
  * The "natural" ordering of this class is assumed to
  * be by score, where a higher score is equivalent to earlier
  * in the order. Use an Ordering typed to this class
  * for different ordering behavior.
  *
  * As this class only contains a simple score, the
  * convenience "+=" method accepts doubles. There is
  * no other direct update method. Other ScoredObject
  * implementations should define their own update methods,
  * if needed.
  */
case class ScoredDocument(
  val id: InternalId,
  var score: Double,
  var name: String = "unknown",
  var rank: Int = 0)
    extends ScoredObject[ScoredDocument] {

  /** Compares two ScoredDocuments by score, breaks ties w/ Docid. */
  def compare(that: ScoredDocument): Int =
    if (that.score < this.score) return -1
    else if (that.score > this.score) return 1
    else (this.id.underlying - that.id.underlying)

  def equals(that: ScoredDocument): Boolean =
    this.id == that.id && this.score == that.score
  def +=(scoreToAdd: Double): Unit = score += scoreToAdd
  def -=(scoreToSubtract: Double): Unit = score -= scoreToSubtract
}
