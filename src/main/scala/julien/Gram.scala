package julien

import scala.math.Ordered

/** Holds a "score" for a particular term in the collection.
  * Useful for term-based expansion techniques.
  */
case class Gram(term: String, score: Double) extends Ordered[Gram] {
  def compare(that: Gram) = {
    val result = that.score compare this.score
    if (result == 0) this.term compare that.term else result
  }
}
