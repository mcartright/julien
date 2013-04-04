package edu.umass.ciir.julien

import scala.collection.mutable.TreeSet

object Synonym {
  def apply(terms: Term*) = new Synonym(terms)
}

class Synonym(terms: Seq[Term])
    extends MultiTermOp(terms)
    with PositionsOp {
  override def positions: Positions = {
    // Assumption: a position is a one-to-one with a word, so the set union of
    // the position vectors of all involved terms is equal to the multiset union
    //
    // Based on that, a TreeSet is a sorted set implementation to hold results.
    // Boom.
    val hits = TreeSet[Int]()
    for (it <- terms.map(t => Positions(t.underlying.extents).iterator)) {
      hits ++= it
    }
    Positions(hits.toArray)
  }
}
