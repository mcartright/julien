package edu.umass.ciir.julien

object BM25 {
  def apply(op: CountOp): BM25 = apply(op, 0.75, 1.2)
  def apply(op: CountOp, b: Double, k: Double): BM25 =
    new BM25(op, b, k)

  def apply(t: Term, b: Double = 0.75, k: Double = 1.2): BM25 =
    apply(new SingleTermOp(t), b, k)
}

class BM25(val op: CountOp, b: Double, k: Double)
    extends TraversableEvaluator[Document]
    with LengthsEvaluator {
  lazy val children: Seq[Operator] = List[Operator](op)
  def views: Set[ViewOp] = Set[ViewOp](op)

  // Runs when asked for the first time, and runs only once
  lazy val stats = op.statistics
  lazy val avgDocLength = stats.collLength / stats.numDocs
  lazy val idf = scala.math.log(stats.numDocs / (stats.docFreq + 0.5))

  def eval(l: Length): Score = eval(op.count, l)
  def eval(d: Document): Score = eval(d.count(op), d.length)
  def eval(c: Count, l: Length): Score = {
    val num = c + (k + 1)
    val den = c + (k * (1 - b + (b * l / avgDocLength)))
    new Score(idf * num / den)
  }
}

