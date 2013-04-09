package julien

/** Provides an [[scala.math.Ordering]] for [[Gram]]s. */
object GramOrdering extends Ordering[Gram] {
  def compare(a: Gram, b: Gram) = {
    val result = b.score compare a.score
    if (result == 0) a.term compare b.term else result
  }
}
