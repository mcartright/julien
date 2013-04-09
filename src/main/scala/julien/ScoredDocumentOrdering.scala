package julien

/** Provides an ordering for [[ScoredDocument]]s. */
object ScoredDocumentOrdering extends Ordering[ScoredDocument] {
  def compare(a: ScoredDocument, b: ScoredDocument) = b.score compare a.score
}
