package julien
package retrieval

/** A specialized Tuple for a scored document.
  * Holds the id of the document and the score.
  *
  * The "natural" ordering of this class is assumed to
  * be by score. Use an Ordering typed to this class
  * for different ordering behavior.
  *
  * As this class only contains a simple score, the
  * convenience "+=" method accepts doubles. There is
  * no other direct update method. Other ScoredObject
  * implementations should define their own update methods,
  * if needed.
  */
case class ScoredDocument(val docid: Docid, var score: Double)
    extends ScoredObject[ScoredDocument] {

  /** Compares two ScoredDocuments by score. */
  def compare(that: ScoredDocument): Int = this.score compare that.score
  def +=(scoreToAdd: Double): Unit = score += scoreToAdd
  def -=(scoreToSubtract: Double): Unit = score -= scoreToSubtract
}