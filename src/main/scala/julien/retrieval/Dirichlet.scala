package julien
package retrieval

object Dirichlet {
  def apply(op: CountView, l: LengthsView): Dirichlet = apply(op, l, 1500)
  def apply(op: CountView, l: LengthsView, mu: Double): Dirichlet =
    new Dirichlet(op, l, mu)
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

  override lazy val upperBound: Score = {
    val maxtf = op.statistics.max
    score(maxtf, maxtf)
  }

  // this is a filthy estimation - want something better
  override lazy val lowerBound: Score = score(0, 600)

  def eval: Score = score(op.count, lengths.length)
  def score(c: Count, l: Length):Score = {
    val num = c + (mu*cf)
    val den = l + mu
    new Score(scala.math.log(num / den))
  }
}
