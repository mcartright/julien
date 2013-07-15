package julien
package retrieval
package processor

import julien.eval.{QueryResult,QueryResultSet}
import julien.behavior.Movable
import scala.collection.Map

object ParallelProcessor {
  def apply[T <: ScoredObject](
    models: Seq[Feature],
    accGen: => Accumulator[T]
  ) = {
    val keysToModels = models.zipWithIndex.map { case (m, i) =>
        (i.toString, m)
    }.toMap
    new ParallelProcessor[T](keysToModels, accGen)
  }
}

/** Used as a simple optimization for queries with shared movable views.
  * The views are updated once for each doc, and all models are evaluated
  * in parallel.
  */
class ParallelProcessor[T <: ScoredObject] private[processor] (
  keysToModels: Map[String, Feature],
  accGen: => Accumulator[T]
)
    extends MultiQueryProcessor[T]
{
  def run(): QueryResultSet[T] = {
    import QueryProcessor.isDone

    // Set up a reverse lookup
    val modelsToKeys = keysToModels.map(_.swap)

    // Extract models (parallel list)
    val models = modelsToKeys.keys.par

    // Set up map of accumulators
    val accs = models.map(m => (m, accGen)).toMap

    // extract movables
    val iterators: Array[Movable] = models.head.movers.distinct.toArray
    val drivers: Array[Movable] = iterators.filterNot(_.isDense).toArray

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
      for (m <- models) {
        // Time to hack it for now
        val hackedAcc = accs(m).asInstanceOf[Accumulator[ScoredDocument]]
        val score = m.eval(candidate)
        hackedAcc += ScoredDocument(candidate, score)
      }


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

    // Done - finalize results
    val results = accs.map { case (m,a) =>
        (modelsToKeys(m), QueryResult(a.result))
    }.toMap
    return QueryResultSet(results.seq)
  }
}
