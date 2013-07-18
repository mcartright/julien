package julien
package retrieval
package processor

import julien._
import julien.eval.QueryResult
import julien.behavior._
import collection.mutable
import collection.mutable.ArrayBuffer

object SimpleProcessor {
  def apply[T <: ScoredObject](
    r: Feature,
    a: Accumulator[T]
  ): SimpleProcessor[T] = new SimpleProcessor(r, a)
  def canProcess(root: Feature): Boolean = true
}

/** The simplest form of query processor. This processor assumes the following:
  *
  * - Only 1 index is provided for execution.
  * - Currently, `SimpleProcessor` only recognizes
  * [[julien.behavior.Movable Movable]]s for scoring organization. This may
  * change in the future.
  *
  * If any of these assumptions are not met, then a different
  * [[julien.retrieval.processor.QueryProcessor QueryProcessor]] should be used.
  */
class SimpleProcessor[T <: ScoredObject] private[processor] (
  val root: Feature,
  acc: Accumulator[T]
)
    extends SingleQueryProcessor[T] {
  def run(): QueryResult[T] = {
    debug("Evaluating", "simpleproc")
    import QueryProcessor.isDone

    // extract iterators, make sure they're reset
    val iterators: Array[Movable] = root.movers.distinct.toArray
    for (it <- iterators) it.reset

    val drivers: Array[Movable] = iterators.filterNot(_.isDense).toArray
    debug(s"drivers: ${drivers.mkString(",")}", "simple")

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
      debug(s"next candidate: $candidate", "simple")
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
      debug(s"Scored $candidate -> $score", "simple")
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
