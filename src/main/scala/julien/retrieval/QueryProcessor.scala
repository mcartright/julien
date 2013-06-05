package julien
package retrieval

import julien.eval.{QueryResult, QueryResultSet}

/** Generic definition of a query processor. */
trait QueryProcessor {
  type DebugHook =
  (ScoredDocument, Seq[Feature], Index, QueryProcessor) => Unit
  type GHook = IteratedHook[_ <: GIterator]

  protected var _indexes = Set[Index]()
  protected var _models = Seq[Feature]()
  def add(f: Feature*) { _models = f ++: _models }
  def indexes: Set[Index] = _indexes
  def models: Seq[Feature] = _models

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
    _models = List[Feature]()
  }

  // This probably needs work -- should probably refactor to objects as
  // a "canProcess" check - will help with Processor selection.
  def validated: Boolean = {
    assume(!_models.isEmpty, s"Trying to validate with no models!")
    _indexes = _models.flatMap(_.hooks).map(_.index).toSet
    return true
  }

  def isBounded(op: Feature): Boolean =
    op.upperBound < Double.PositiveInfinity &&
  op.lowerBound > Double.NegativeInfinity

  final def isDone(drivers: Array[Movable]): Boolean = {
    var j = 0
    while (j < drivers.length) {
      if (!drivers(j).isDone) return false
      j += 1
    }
    return true
  }

  final def matches(drivers: Array[Movable], candidate: Int): Boolean = {
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
