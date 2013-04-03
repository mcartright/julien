package edu.umass.ciir.julien

object JelinekMercer {
  def apply(op: CountOp, l: LengthsOp): JelinekMercer = apply(op, l, 1500)
  def apply(op: CountOp, l: LengthsOp, lambda: Double): JelinekMercer =
    new JelinekMercer(op, l, lambda)
  def apply(t: Term, l: LengthsOp, lambda: Double = 1500): JelinekMercer =
    apply(new SingleTermOp(t), l, lambda)
}

class JelinekMercer(val op: CountOp, val lengths: LengthsOp, lambda: Double) {
  lazy val children: Seq[Operator] = List[Operator](op, lengths)
  lazy val views: Set[ViewOp] = Set[ViewOp](op, lengths)

  // Runs when asked for the first time, and runs only once
  lazy val cf = {
    val stats: CountStatistics = op.statistics
    ((stats.collFreq + 0.5) / stats.collLength)
  }

  def eval: Score = {
    val foreground = op.count / lengths.length
    new Score(scala.math.log((lambda*foreground) + ((1.0-lambda)*cf)))
  }
}


