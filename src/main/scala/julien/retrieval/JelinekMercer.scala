package julien
package retrieval

object JelinekMercer {
  def apply(op: CountView, l: LengthsView): JelinekMercer = apply(op, l, 1500)
  def apply(op: CountView, l: LengthsView, lambda: Double): JelinekMercer =
    new JelinekMercer(op, l, lambda)
  def apply(t: Term, l: LengthsView, lambda: Double = 1500): JelinekMercer =
    apply(new SingleTermView(t), l, lambda)
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

  def eval: Score = {
    val foreground = op.count / lengths.length
    new Score(scala.math.log((lambda*foreground) + ((1.0-lambda)*cf)))
  }
}


