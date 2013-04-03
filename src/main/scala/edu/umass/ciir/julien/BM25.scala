package edu.umass.ciir.julien

object BM25 {
  def apply(op: CountOp, l: LengthsOp): BM25 = apply(op, l, 0.75, 1.2)
  def apply(op: CountOp, l: LengthsOp, b: Double, k: Double): BM25 =
    new BM25(op, l, b, k)

  def apply(t: Term, l: LengthsOp, b: Double = 0.75, k: Double = 1.2): BM25 =
    apply(new SingleTermOp(t), l, b, k)
}

class BM25(op: CountOp, lengths: LengthsOp,  b: Double, k: Double) {
  lazy val children: Seq[Operator] = List[Operator](op, lengths)
  lazy val views: Set[ViewOp] = Set[ViewOp](op, lengths)

  // Runs when asked for the first time, and runs only once
  lazy val stats = op.statistics
  lazy val avgDocLength = stats.collLength / stats.numDocs
  lazy val idf = scala.math.log(stats.numDocs / (stats.docFreq + 0.5))

  def eval: Score = {
    val num = op.count + (k + 1)
    val den = op.count + (k * (1 - b + (b * lengths.length / avgDocLength)))
    new Score(idf * num / den)
  }
}

