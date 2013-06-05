package julien
package retrieval

import julien._
import julien.eval.QueryResult
import collection.mutable
import collection.mutable.ArrayBuffer

object SimpleProcessor {
  def apply() = new SimpleProcessor()
}

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
class SimpleProcessor
  extends QueryProcessor {
  var debugger: Option[DebugHook] = None

  override def validated: Boolean = {
    val looseCheck = super.validated
    if (looseCheck == false) return looseCheck

    // For this processor, let's assume only 1 index may be held
    // Other processors will do trickier stuff
    assume(_indexes.size == 1,
      s"${toString} does not process more than 1 index at a time.")
    return true
  }

  def prepare(): Unit = {
    val unprepped: Set[Operator] =
      models.flatMap(m =>
        m.filter(_.isInstanceOf[NeedsPreparing])).toSet
    if (unprepped.size > 0) {
      // We now need to get the iterators of the unprepped nodes, zip down them
      // and update statistics until done, then reset.
      val iterators: Array[Movable] =
        unprepped.flatMap(_.movers).toSet.filterNot(_.isDense).toArray

      while (!isDone(iterators)) {
        val activeBuf = Array.newBuilder[Movable]
        var l=0
        while (l < iterators.length) {
          val curItr = iterators(l)
          if (!curItr.isDone) {
            activeBuf += curItr
          }
          l += 1
        }
        val active = activeBuf.result()

        val numActive = active.length
        //val candidate = active.map(_.at).min
        var k=0
        var candidate = Int.MaxValue
        while (k < numActive) {
          val curVal = active(k).at
          if (curVal < candidate) {
            candidate = curVal
          }
          k += 1
        }

        var i = 0
        while (i < numActive) {
          active(i).moveTo(candidate)
          i += 1
        }

        for (p <- unprepped) {
          p.asInstanceOf[NeedsPreparing].updateStatistics(InternalId(candidate))
        }

        var j = 0
        while (j < numActive) {
          active(j).movePast(candidate)
          j += 1
        }
      }
      unprepped.foreach(_.asInstanceOf[NeedsPreparing].prepared)
    }

    // Do this regardless in case any iterators are recycled.
    val movers = models.flatMap(_.movers).toSet
    movers.foreach(_.reset)
  }

  def run[T <: ScoredObject[T]](
    acc: Accumulator[T] = DefaultAccumulator[ScoredDocument]()
  ): QueryResult[T] = {
    // Make sure we can do the next stuff easily
    assume(validated, s"Unable to validate given model/index combination")
    prepare()

    // extract iterators
    val index = _indexes.head
    val model = _models.head
    val iterators: Array[Movable] = _models.flatMap(_.movers).distinct.toArray
    val drivers: Array[Movable] = iterators.filterNot(_.isDense).toArray
    val scorers: Array[Feature] = _models.toArray

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
      if (debugger.isDefined) debugger.get(sd, scorers, index, this)

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
