package julien
package retrieval

import julien.eval.{QueryResult, QueryResultSet}
import julien.retrieval._
import julien.behavior._

/** Factory for creating QueryProcessor instances. The apply method will
  * eventually build up to act as a large DFA structure for choosing the
  * most appropriate processor for a given query.
  */
object QueryProcessor {
  /** Selects a QueryProcessor for the given query. */
  def apply(root: Feature): QueryProcessor = {
  }
}


/** Generic definition of a query processor instance. */
trait QueryProcessor {
  type DebugHook =
  (ScoredDocument, Seq[Feature], Index, QueryProcessor) => Unit

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
