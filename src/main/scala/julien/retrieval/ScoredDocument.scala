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
    extends ScoredObject {

  def equals(that: ScoredDocument): Boolean =
    this.id == that.id && this.score == that.score
  def +=(scoreToAdd: Double): Unit = score += scoreToAdd
  def -=(scoreToSubtract: Double): Unit = score -= scoreToSubtract
}

object ScoredDocumentDefaultOrdering extends Ordering[ScoredDocument] {
  /** Compares two ScoredDocuments by score, breaks ties w/ Docid. */
  def compare(sd1: ScoredDocument, sd2: ScoredDocument): Int =
    if (sd2.score < sd1.score) return -1
    else if (sd2.score > sd1.score) return 1
    else (sd1.id.underlying - sd2.id.underlying)
}
