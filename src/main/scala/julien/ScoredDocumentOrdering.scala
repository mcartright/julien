package julien

/** Provides an ordering for [[ScoredDocument]]s. */
object ScoredDocumentOrdering extends Ordering[ScoredDocument] {
  def compare(a: ScoredDocument, b: ScoredDocument) =
    // TODO: This works, but gross.
    b.score.underlying compare a.score.underlying
}
