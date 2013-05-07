package julien
package retrieval

import julien._
import julien.eval.QueryResult

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
      val iterators: Set[GHook] =
        unprepped.flatMap(_.iHooks).toSet.filterNot(_.isDense)

      while (iterators.exists(!_.isDone)) {
        val active = iterators.filterNot(_.isDone)
        val candidate = active.map(_.at).min
        iterators.foreach(_.moveTo(candidate))
        if (iterators.exists(_.matches(candidate))) {
          for (p <- unprepped) {
            p.asInstanceOf[NeedsPreparing].
              updateStatistics(InternalId(candidate))
          }
        }
        active.foreach(_.movePast(candidate))
      }
      unprepped.foreach(_.asInstanceOf[NeedsPreparing].prepared)
    }

    // Do this regardless in case any iterators are recycled.
    val hooks = models.flatMap(_.iHooks).toSet
    for (h <- hooks) h.underlying.reset
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
    val iterators: Array[GHook] = _models.flatMap(_.iHooks).toArray
    val drivers: Array[GHook] = iterators.filterNot(_.isDense).toArray

    // Need to fix this
    val scorers: Array[FeatureOp] = _models.toArray

    // Go
    while (!isDone(drivers)) {
      val candidate = drivers.foldLeft(Int.MaxValue) {
        (best, drv) =>
          if (drv.isDone) best else scala.math.min(drv.at, best)
      }

      var i = 0
      while (i < iterators.length) {
        iterators(i).moveTo(candidate)
        i += 1
      }

      if (matches(drivers, candidate)) {
        // Time to score...using a gross loop
        i = 0
        var score = 0.0
        while (i < scorers.length) {
          score += scorers(i).eval
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
      }

      var j = 0
      while (j < drivers.length) {
        drivers(j).movePast(candidate)
        j += 1
      }
    }
    QueryResult(acc.result)
  }
}
