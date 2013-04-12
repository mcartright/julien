package julien
package retrieval

object BM25 {
  def apply(op: CountView, l: LengthsView): BM25 = apply(op, l, 0.75, 1.2)
  def apply(op: CountView, l: LengthsView, b: Double, k: Double): BM25 =
    new BM25(op, l, b, k)
}

class BM25(op: CountView, lengths: LengthsView,  b: Double, k: Double) {
  lazy val children: Seq[Operator] = List[Operator](op, lengths)
  lazy val views: Set[ViewOp] = Set[ViewOp](op, lengths)

  // Runs when asked for the first time, and runs only once
  lazy val stats = op.statistics
  lazy val avgDocLength = stats.collLength / stats.numDocs
  lazy val idf = scala.math.log(stats.numDocs / (stats.docFreq + 0.5))

  // Yay - plays nice w/ the bounds.
  val lowerBound: Score = new Score(0)
  lazy val upperBound: Score = {
    val maxtf = op.statistics.max
    score(maxtf, maxtf)
  }

  def eval: Score = score(op.count, lengths.length)
  def score(c: Count, l: Length) = {
    val num = c + (k + 1)
    val den = c + (k * (1 - b + (b * l / avgDocLength)))
    new Score(idf * num / den)
  }
}

