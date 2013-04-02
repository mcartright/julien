package edu.umass.ciir.julien

object Dirichlet {
  def apply(op: CountOp): Dirichlet = apply(op, 1500)
  def apply(op: CountOp, mu: Double): Dirichlet =
    new Dirichlet(op, mu)

  def apply(t: Term, mu: Double = 1500): Dirichlet =
    apply(new SingleTermOp(t), mu)
}

class Dirichlet(val op: CountOp, mu: Double)
    extends TraversableEvaluator[Document] {
  lazy val children: Seq[Operator] = List[Operator](op)
  def views: Set[ViewOp] = Set[ViewOp](op)
  // Runs when asked for the first time, and runs only once
  lazy val cf = {
    val stats: CountStatistics = op.statistics
    ((stats.collFreq + 0.5) / stats.collLength.toDouble)
  }

  def eval(d: Document): Score = {
    val num = d.count(op) + (mu*cf)
    val den = d.length + mu
    new Score(scala.math.log(num / den))
  }
}
