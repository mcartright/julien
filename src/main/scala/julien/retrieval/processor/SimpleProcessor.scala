package julien
package retrieval
package processor

import julien._
import julien.eval.QueryResult
import julien.behavior._
import collection.mutable
import collection.mutable.ArrayBuffer

/** The simplest form of query processor. This processor assumes the following:
  *
  * - Only 1 index is provided for execution.
  * - None of the `View` opeators have a [[julien.retrieval.Slicer Slicer]]
  * attached.
  * - Currently, `SimpleProcessor` only recognizes
  * [[julien.retrieval.IteratedHook IteratedHooks]]. This may
  * change in the future.
  *
  * If any of these assumptions are not met, then a different
  * [[julien.retrieval.QueryProcessor QueryProcessor]] should be used.
  */
class SimpleProcessor private[processor] (val root: Feature)
    extends QueryProcessor
{
  def run[T <: ScoredObject[T]](acc: Accumulator[T]): QueryResult[T] = {
    import QueryProcessor.isDone

    // extract iterators
    val iterators: Array[Movable] = root.movers.distinct.toArray
    val drivers: Array[Movable] = iterators.filterNot(_.isDense).toArray
    val scorers: Array[Feature] = Array(root)

    // Go

    // Use this outside the loop once, then we update *when* we move.
    var i = 0
    var candidate = Int.MaxValue
    while (i < drivers.length) {
      val drv = drivers(i)
      if (!drv.isDone) {
        if (drv.at < candidate) candidate = drv.at
      }
      i += 1
    }

    while (!isDone(drivers)) {
      // At this point the next candidate should be chosen
      // Time to score
      var score = 0.0
      i = 0
      while (i < scorers.length) {
        score += scorers(i).eval(candidate)
        i += 1
      }

      // How do we instantiate an object without knowing what it is, and
      // knowing what it needs? One method in the QueryProcessor?

      // For now, put a nasty hack in to make it work.
      // SAFETY OFF
      val hackedAcc = acc.asInstanceOf[Accumulator[ScoredDocument]]
      val sd = ScoredDocument(candidate, score)
      hackedAcc += sd

      // As we move forward, set the candidate since movePast reports it
      i = 0
      var newCandidate = Int.MaxValue
      while (i < drivers.length) {
        if (!drivers(i).isDone) {
          val tmp = drivers(i).movePast(candidate)
          if (tmp < newCandidate) newCandidate = tmp
        }
        i += 1
      }

      // Update the next candidate to score
      candidate = newCandidate
    }
    QueryResult(acc.result)
  }
}