package edu.umass.ciir.julien

object Dirichlet {
  def apply(op: CountView, l: LengthsView): Dirichlet = apply(op, l, 1500)
  def apply(op: CountView, l: LengthsView, mu: Double): Dirichlet =
    new Dirichlet(op, l, mu)

  def apply(t: Term, l: LengthsView, mu: Double = 1500): Dirichlet =
    apply(new SingleTermView(t), l, mu)
}

class Dirichlet(op: CountView, lengths: LengthsView, mu: Double)
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
