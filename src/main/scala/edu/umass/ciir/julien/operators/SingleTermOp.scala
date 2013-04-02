package operators

object SingleTermOp {
  def apply(t: Term) = new SingleTermOp(t)
}

class SingleTermOp(val t: Term) extends PositionsOp {
  def count: Count = new Count(t.underlying.count)
  def positions: Positions = Positions(t.underlying.extents())
}
