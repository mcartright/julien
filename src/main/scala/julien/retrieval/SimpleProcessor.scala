package julien
package retrieval

import scala.collection.mutable.PriorityQueue
import julien._

object SimpleProcessor {
  def apply() = new SimpleProcessor()
}

class SimpleProcessor extends QueryProcessor {
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
      models.flatMap(m => m.filter(_.isInstanceOf[NeedsPreparing])).toSet
    if (unprepped.size == 0) return // Got lucky, all done!

    // We now need to get the iterators of the unprepped nodes, zip down them
    // and update statistics until done, then reset.
    val iterators: Set[GIterator] = unprepped
      .flatMap(_.children)
      .filter(_.isInstanceOf[IteratedHook[_ <: GIterator]])
      .map(_.asInstanceOf[IteratedHook[_ <: GIterator]].underlying)
      .toSet
      .filterNot(_.hasAllCandidates)

    while (iterators.exists(!_.isDone)) {
      val active = iterators.filterNot(_.isDone)
      val candidate = active.map(_.currentCandidate).min
      iterators.foreach(_.syncTo(candidate))
      if (iterators.exists(_.hasMatch(candidate))) {
        unprepped.foreach(_.asInstanceOf[NeedsPreparing].updateStatistics)
      }
      active.foreach(_.movePast(candidate))
    }

    // Should be all done - reset the iterators
    iterators.map(_.reset)
  }

  def run: List[ScoredDocument] = {
    // Make sure we can do the next stuff easily
    assume(validated, s"Unable to validate given model/index combination")
    prepare()

    // extract iterators
    val index = _indexes.head
    val model = _models.head
    val iterators: Set[GIterator] = model.iHooks.map(_.underlying).toSet
      model.filter(_.isInstanceOf[IteratedHook[_ <: GIterator]]).map { t =>
        t.asInstanceOf[IteratedHook[_ <: GIterator]].underlying
    }.toSet.filterNot(_.hasAllCandidates)
    // Need to fix this
    val scorers : List[FeatureOp] = List[FeatureOp](model)

    // Go
    val numResults: Int = 100
    val resultQueue = PriorityQueue[ScoredDocument]()
    while (iterators.exists(_.isDone == false)) {
      val candidate = iterators.filterNot(_.isDone).map(_.currentCandidate).min
      iterators.foreach(_.syncTo(candidate))
      if (iterators.exists(_.hasMatch(candidate))) {
        // Time to score
        debug("scoring candidate: " + candidate + " " + index.name(candidate))
        val score = scorers.map(_.eval).sum
        resultQueue.enqueue(ScoredDocument(candidate, score))
        if (resultQueue.size > numResults) resultQueue.dequeue
      }
      iterators.foreach(_.movePast(candidate))
    }
    resultQueue.reverse
  }
}
