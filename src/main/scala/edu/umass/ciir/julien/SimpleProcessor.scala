package edu.umass.ciir.julien

import edu.umass.ciir.julien.Utils._
import scala.collection.mutable.PriorityQueue

abstract class QueryProcessor {
  protected var _indexes = List[Index]()
  protected var _models = List[FeatureOp]()
  var numResults: Int = 100
  def add(i: Index) { _indexes = i :: indexes }
  def add(f: FeatureOp*) { _models = f ++: models }
  def indexes: List[Index] = _indexes
  def models: List[FeatureOp] = _models

  // The thing that needs implementing in subclasses
  def run: List[ScoredDocument]

  // This probably needs work -- should probably refactor to objects as
  // a "canProcess" check - will help with Processor selection.
  def validated: Boolean = {
    assume(!_models.isEmpty, s"Trying to validate with no models!")

    // Need to verify that all model hooks have attachments.
    // Also will guarantee that if any hooks are attached to an
    // unknown index, it's added to the list of known indexes.
    for (m <- _models) {
      val hooks =
        m.filter(_.isInstanceOf[IndexHook]).map(_.asInstanceOf[IndexHook])

      // Conditionally try to hook up if needed
      if (!hooks.forall(_.isAttached) && _indexes.size == 1) {
        hooks.filter(!_.isAttached).foreach(h => h.attach(_indexes(0)))
      }

      if (hooks.exists(!_.isAttached)) return false
      val newIndexes =
        hooks map {
          _.attachedIndex
        } filterNot {
          i => _indexes.contains(i)
        }
      _indexes = newIndexes ::: _indexes
    }
    return true
  }
}

object SimpleProcessor {
  def apply() = new SimpleProcessor()
}

class SimpleProcessor extends QueryProcessor {
  override def validated: Boolean = {
    val looseCheck = super.validated
    if (!looseCheck) return looseCheck

    // For this processor, let's assume only 1 index may be held
    // Other processors will do trickier stuff
    return _indexes.size == 1
  }

  def run: List[ScoredDocument] = {
    assume(validated, s"Unable to validate given model/index combination")
        // extract iteators
    val index = _indexes(0)
    val model = _models(0)
    val iterators = model.filter(_.isInstanceOf[Term]).map { t =>
      t.asInstanceOf[Term].underlying
    }
    val lengths = index.lengthsIterator
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
        lazy val currentdoc =
          index.document(index.underlying.getName(candidate))
        val len = new Length(lengths.getCurrentLength)
        var score = scorers.foldRight(new Score(0.0)) { (S,N) =>
          S match {
            case i: IntrinsicEvaluator => i.eval + N
            case l: LengthsEvaluator => l.eval(len) + N
            case t: TraversableEvaluator[Document] => t.eval(currentdoc) + N
            case _ => N
          }
        }
        resultQueue.enqueue(ScoredDocument(candidate, score.underlying))
        if (resultQueue.size > numResults) resultQueue.dequeue
      }
      iterators.foreach(_.movePast(candidate))
    }
    resultQueue.reverse
  }
}
