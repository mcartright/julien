package julien
package retrieval

import julien._
import julien.galago.tupleflow.Utility
import julien.galago

object SimpleProcessor {
  def apply() = new SimpleProcessor()
}

/** The simplest form of query processor. This processor assumes the following:
  *
  * - Only 1 index is provided for execution.
  * - None of the `View` opeators have a [[julien.retrieval.Slicer Slicer]]
  *   attached.
  * - Currently, `SimpleProcessor` only recognizes
  *  [[julien.retrieval.IteratedHook IteratedHook IteratedHooks]]. This may
  *  change in the future.
  *
  * If any of these assumptions are not met, then a different
  * [[julien.retrieval.QueryProcessor QueryProcessor]] should be used.
  */
class SimpleProcessor extends QueryProcessor {
  type GHook = IteratedHook[_ <: GIterator]

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
          unprepped.foreach(_.asInstanceOf[NeedsPreparing].updateStatistics)
        }
        active.foreach(_.movePast(candidate))
      }
      unprepped.foreach(_.asInstanceOf[NeedsPreparing].prepared)
    }

    // Do this regardless in case any iterators are recycled.
    val hooks = models.flatMap(_.iHooks).toSet
    for (h <- hooks) h.underlying.reset
  }

  def run[T <: ScoredObject[T]](acc: Accumulator[T] =
    DefaultAccumulator[ScoredDocument]): List[T] = {
    // Make sure we can do the next stuff easily
    assume(validated, s"Unable to validate given model/index combination")
    prepare()

    // extract iterators
    val index = _indexes.head
    val model = _models.head
    val iterators: Set[GHook] = _models.
      flatMap(_.iHooks).toSet
    val drivers: Set[GHook] = iterators.filterNot(_.isDense)

    // Need to fix this
    val scorers : List[FeatureOp] = _models

    val scoreMap =
      scala.collection.mutable.HashMap[FeatureOp, Tuple3[Int,Double,Int]]()

    // Go
    while (drivers.exists(_.isDone == false)) {
      val candidate = drivers.filterNot(_.isDone).map(_.at).min
      iterators.foreach(_.moveTo(candidate))
      if (drivers.exists(_.matches(candidate))) {
        // Time to score
        for (f <- scorers.head.children) {
          val tc = f.grab[Term].head
          if (!scoreMap.contains(f)) scoreMap(f) = (tc.at, f.eval, tc.count)
          else if (scoreMap(f)._2 < f.eval) scoreMap(f) = (tc.at, f.eval, tc.count)
        }
        val score = scorers.map(_.eval).sum
        // How do we instantiate an object without knowing what it is, and
        // knowing what it needs? One method in the QueryProcessor?

        // For now, put a nasty hack in to make it work.
        // SAFETY OFF
        val hackedAcc = acc.asInstanceOf[Accumulator[ScoredDocument]]
        hackedAcc += ScoredDocument(candidate, score)
      }
      drivers.foreach(_.movePast(candidate))
    }
    scoreMap.foreach { case (feat, result) =>
        println(s"ACTUAL: $feat => $result")
    }
    acc.result
  }
}
