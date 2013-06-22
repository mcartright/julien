package julien
package retrieval
package processor

import julien.eval.{QueryResult, QueryResultSet}
import julien.retrieval._
import julien.behavior._

/** Factory for creating QueryProcessor instances. The apply method will
  * eventually build up to act as a large DFA structure for choosing the
  * most appropriate processor for a given query.
  */
object QueryProcessor {
  private case class Counter(
    var numNeedsPreparing: Int = 0,
    var numBounded: Int = 0,
    var numConjunction: Int = 0,
    var numDistributive: Int = 0,
    var numFinite: Int = 0,
    var numMovable: Int = 0,
    var numRandomAccess: Int = 0,
    var all : Int = 0
  )

  /** Selects a QueryProcessor to run all of the models in
    * parallel - and 'in parallel' means that for each
    * document, the processor will move once, and evaluate
    * many times.
    */
  def apply(models: Seq[Feature]): QueryProcessor = ???

  /** Selects a QueryProcessor for the given query. */
  def apply(root: Feature): QueryProcessor = {

    // First gather statistics - is there any way to do this that isn't
    // a code smell?
    val c = Counter()
    for (op <- root) {
      c.all += 1
      if (op.isInstanceOf[NeedsPreparing]) c.numNeedsPreparing += 1
      if (op.isInstanceOf[Bounded]) c.numBounded += 1
      if (op.isInstanceOf[Conjunction]) c.numConjunction += 1
      if (op.isInstanceOf[Distributive]) c.numDistributive += 1
      if (op.isInstanceOf[Finite]) c.numFinite += 1
      if (op.isInstanceOf[Movable]) c.numMovable += 1
      if (op.isInstanceOf[RandomAccess]) c.numRandomAccess += 1
    }

    if (c.numNeedsPreparing > 0) {
      new SimpleProcessor(root) with Preparer {
        override def run[T <: ScoredObject[T]](
          acc: Accumulator[T]
        ): QueryResult[T] = {
          prepare()
          super.run(acc)
        }
      }
    } else {
      new SimpleProcessor(root)
    }
  }

/** Was the check for the preloading processors. Need to reactivate it somehow.
    // Structural check for something like:
    // Combine(f1, f2, f3, ...)
    // All top-level operators in _models should look like this.
    var result: Boolean = _models.forall(_.isInstanceOf[Distributive])
    // Make sure for each combiner, each child is a feature with
    // actual bounds.
    for (combiner <- _models) {
      // Do children explicitly so we don't traverse the entire
      // subtree rooted here.
      result = result && combiner.children.forall { child =>
        child.isInstanceOf[Feature] &&
        child.isInstanceOf[Finite]
        child.movers.filter(_.isSparse).size == 1 // make sure it's 1-to-1
      }
    }
 */

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


/** Generic definition of a query processor instance. */
trait QueryProcessor {
  // Pull in static functions
  import QueryProcessor.{isDone,matches}

  type DebugHook =
  (ScoredDocument, Seq[Feature], Index, QueryProcessor) => Unit

  def root: Feature

  // The only abstract method
  def run[T <: ScoredObject[T]](
    acc: Accumulator[T] = DefaultAccumulator[ScoredDocument]()
  ): QueryResult[T]
}
