package julien
package retrieval

object JelinekMercer {
  def apply(op: CountView, l: LengthsView): JelinekMercer = apply(op, l, 1500)
  def apply(op: CountView, l: LengthsView, lambda: Double): JelinekMercer =
    new JelinekMercer(op, l, lambda)
  def apply(t: Term, l: LengthsView, lambda: Double = 1500): JelinekMercer =
    apply(t, l, lambda)
}

class JelinekMercer(
  val op: CountView,
  val lengths: LengthsView,
  lambda: Double)
{
  lazy val children: Seq[Operator] = List[Operator](op, lengths)
  lazy val views: Set[ViewOp] = Set[ViewOp](op, lengths)

  // Runs when asked for the first time, and runs only once
  lazy val cf = {
    val stats: CountStatistics = op.statistics
    ((stats.collFreq + 0.5) / stats.collLength)
  }

  lazy val upperBound: Score = {
    val maxtf = op.statistics.max
    score(maxtf, maxtf)
  }

  // Crappy estimate. What's better?
  lazy val lowerBound: Score = score(0, 600)

  def eval: Score = score(op.count, lengths.length)
  def score(c: Count, l: Length) =
    new Score(scala.math.log((lambda*(c/l)) + ((1.0-lambda)*cf)))
}


