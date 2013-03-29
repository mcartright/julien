package edu.umass.ciir.julien

abstract class WindowSource(val sources: List[BoundSource])
    extends BoundSource with Synthetic {

  override def count: Int = positions.size

  def supports: Set[Stored] =
    sources.foldLeft(Set[Stored]()) { case (supps, src) =>
        src match {
          case st: Stored => supps + st
          case sy: Synthetic => supps ++ sy.supports
          case _ => supps
        }
    }
}
