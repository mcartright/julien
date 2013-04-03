package edu.umass.ciir.julien

object Dirichlet {
  def apply(op: CountOp, l: LengthsOp): Dirichlet = apply(op, l, 1500)
  def apply(op: CountOp, l: LengthsOp, mu: Double): Dirichlet =
    new Dirichlet(op, l, mu)

  def apply(t: Term, l: LengthsOp, mu: Double = 1500): Dirichlet =
    apply(new SingleTermOp(t), l, mu)
}

class Dirichlet(op: CountOp, lengths: LengthsOp, mu: Double)
    extends FeatureOp {
  lazy val children: Seq[Operator] = List[Operator](op, lengths)
  def views: Set[ViewOp] = Set[ViewOp](op, lengths)
  // Runs when asked for the first time, and runs only once
  lazy val cf = {
    val stats: CountStatistics = op.statistics
    ((stats.collFreq + 0.5) / stats.collLength)
  }

  def eval: Score = {
    val num = op.count + (mu*cf)
    val den = lengths.length + mu
    new Score(scala.math.log(num / den))
  }
}
