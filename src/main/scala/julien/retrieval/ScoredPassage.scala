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
  val id: Int,
  var score: Double,
  val begin: Int,
  val end: Int,
  var name: String  = "unknown",
  var rank: Int = 0) extends ScoredObject {
  assume(begin < end, s"Can't have a scored passage with bad indices.")

  def +=(scoreToAdd: Double): Unit = score += scoreToAdd
  def -=(scoreToSubtract: Double): Unit = score -= scoreToSubtract
}

object ScoredPassageDefaultOrdering extends Ordering[ScoredPassage] {

  /** Compares passages by score. */
  def compare(sp1: ScoredPassage, sp2: ScoredPassage): Int =
    if (sp2.score < sp1.score) return -1
    else if (sp2.score > sp1.score) return 1
    else (sp1.id - sp2.id)
}
