package julien
package retrieval

import julien.eval.{QueryResult, QueryResultSet}

/** Generic definition of a query processor. */
trait QueryProcessor {
  type DebugHook =
  (ScoredDocument, Seq[FeatureOp], Index, QueryProcessor) => Unit
  type GHook = IteratedHook[_ <: GIterator]

  protected var _indexes = Set[Index]()
  protected var _models = Seq[FeatureOp]()
  def add(i: Index) { _indexes = _indexes + i }
  def add(f: FeatureOp*) { _models = f ++: _models }
  def indexes: Set[Index] = _indexes
  def models: Seq[FeatureOp] = _models

  // The things that need implementing in subclasses
  // makes sure that all views are ready to provide info upwards
  def prepare: Unit
  def run[T <: ScoredObject[T]](acc: Accumulator[T]): QueryResult[T]
  def runBatch[T <: ScoredObject[T]](
    queries: Map[String, String],
    prep: QueryPreparer,
    acc: Accumulator[T]): QueryResultSet[T] = {
    val results = Map.newBuilder[String, QueryResult[T]]
    for ((qid, q) <- queries) {
      _models = prep(q)
      val result = run(acc)
      results += (qid -> result)
    }
    return QueryResultSet(results.result)
  }

  def clear: Unit = {
    _indexes = Set[Index]()
    _models = List[FeatureOp]()
  }

  // This probably needs work -- should probably refactor to objects as
  // a "canProcess" check - will help with Processor selection.
  def validated: Boolean = {
    assume(!_models.isEmpty, s"Trying to validate with no models!")

    // Need to verify that all model hooks have attachments.
    // Also will guarantee that if any hooks are attached to an
    // unknown index, it's added to the list of known indexes.
    for (m <- _models) {
      val hooks = m.hooks

      // Add all previously unknown indexes from attached hooks
      _indexes = hooks.filter(_.isAttached).map(_.attachedIndex) ++: _indexes

      // Conditionally try to hook up if needed
      if (!hooks.forall(_.isAttached) && _indexes.size == 1) {
        hooks.filter(!_.isAttached).foreach(h => h.attach(_indexes.head))
      }

      assume(hooks.forall(_.isAttached),
        "Unable to implicitly attach all hooks to models.")
    }
    return true
  }

  def isBounded(op: FeatureOp): Boolean =
    op.upperBound < Double.PositiveInfinity &&
  op.lowerBound > Double.NegativeInfinity

  final def isDone(drivers: Array[GHook]): Boolean = {
    var j = 0
    while (j < drivers.length) {
      val curDone = drivers(j).isDone
      if (curDone == false) {
        return false
      }
      j += 1
    }
    return true
  }

  final def matches(drivers: Array[GHook], candidate: Int): Boolean = {
    var j = 0
    while (j < drivers.length) {
      val matches = drivers(j).matches(candidate)
      if (matches == true) {
        return true
      }
      j += 1
    }
    return false
  }
}
