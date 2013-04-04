package edu.umass.ciir.julien

import edu.umass.ciir.julien.Utils._
import scala.collection.mutable.PriorityQueue

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

  def run: List[ScoredDocument] = {
    assume(validated, s"Unable to validate given model/index combination")
        // extract iteators
    val index = _indexes.head
    val model = _models.head
    val iterators = model.filter(_.isInstanceOf[Term]).map { t =>
      t.asInstanceOf[Term].underlying
    }
    val lengths = index.lengthsIterator
    // Need to fix this
    val scorers : List[FeatureOp] = List[FeatureOp](model)

    // Go
    val numResults: Int = 100
    val resultQueue = PriorityQueue[ScoredDocument]()(ScoredDocumentOrdering)
    while (iterators.exists(_.isDone == false)) {
      val candidate = iterators.filterNot(_.isDone).map(_.currentCandidate).min
      lengths.syncTo(candidate)
      iterators.foreach(_.syncTo(candidate))
      if (iterators.exists(_.hasMatch(candidate))) {
        // Time to score
        val len = new Length(lengths.getCurrentLength)
        var score = scorers.foldLeft(new Score(0.0)) { (score,op) =>
          score + op.eval
        }
        resultQueue.enqueue(ScoredDocument(candidate, score.underlying))
        if (resultQueue.size > numResults) resultQueue.dequeue
      }
      iterators.foreach(_.movePast(candidate))
    }
    resultQueue.reverse
  }
}
