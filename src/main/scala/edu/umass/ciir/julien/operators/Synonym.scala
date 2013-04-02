package operators

import scala.collection.mutable.ArrayBuffer
import org.lemurproject.galago.core.util._

object Synonym {
  def apply(terms: Term*) = new Synonym(terms: _*)
}

class Synonym(terms: Term*)
    extends MultiTermOp
    with PositionsOp {
  override def count: Count = new Count(this.positions.size)
  override def positions: Positions = {
    val hits = Positions()
    for (it <- terms.map(t => Positions(t.underlying.extents).iterator)) {
      hits.appendAll(it)
    }
    val f = Positions()
    for (i <- hits) f.append(i)
    return f
  }
}
