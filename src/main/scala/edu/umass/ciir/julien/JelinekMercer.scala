package edu.umass.ciir.julien

object JelinekMercer {
  def apply(op: CountOp): JelinekMercer = apply(op, 1500)
  def apply(op: CountOp, lambda: Double): JelinekMercer =
    new JelinekMercer(op, lambda)

  def apply(t: Term, lambda: Double = 1500): JelinekMercer =
    apply(new SingleTermOp(t), lambda)
}

class JelinekMercer(val op: CountOp, lambda: Double)
    extends TraversableEvaluator[Document]
    with CLEvaluator
    with LengthsEvaluator {
  lazy val children: Seq[Operator] = List[Operator](op)
  def views: Set[ViewOp] = Set[ViewOp](op)

  // Runs when asked for the first time, and runs only once
  lazy val cf = {
    val stats: CountStatistics = op.statistics
    ((stats.collFreq + 0.5) / stats.collLength)
  }

  def eval(l: Length): Score = eval(op.count, l)
  def eval(d: Document): Score = eval(d.count(op), d.length)
  def eval(c: Count, l: Length): Score = {
    val foreground = c / l
    new Score(scala.math.log((lambda*foreground) + ((1.0-lambda)*cf)))
  }
}


