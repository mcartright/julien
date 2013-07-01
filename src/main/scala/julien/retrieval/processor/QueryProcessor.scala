package julien
package retrieval
package processor

import julien.eval.{QueryResult, QueryResultSet}
import julien.retrieval._
import julien.behavior._
import scala.collection.mutable.ArraySeq

/** Factory for creating QueryProcessor instances. The apply method will
  * eventually build up to act as a large DFA structure for choosing the
  * most appropriate processor for a given query.
  */
object QueryProcessor {
  val rewriters: ArraySeq[QueryRewriter] = ArraySeq()

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

  /** Selects a QueryProcessor for each query. If any subset of the
    * batch of queries shares the exact same set of
    * [[julien.behavior.Movable Movables]], those queries are executed
    * in one pass of the index. Otherwise distinct queries are executed
    * independently.
    */
  def apply(models: Seq[Feature]): QueryResultSet[ScoredDocument] = {
    def accGen = DefaultAccumulator[ScoredDocument]()
    this.apply(models, accGen)
  }

  def apply[T <: ScoredObject[T]](
    models: Seq[Feature],
    accGen: => Accumulator[T]
  ): QueryResultSet[T] = {
    val movables = models.head.movers
    // This is the "dumb" check - if they all share the same views,
    // then do them all. We need something in the middle.
    if (models.forall(m => m.movers.size == movables.size)) {
      val resultSet = ParallelProcessor(models, accGen).run()
      return resultSet
    } else {
      // NOT shared correctly - back off to safer but much slower
      // execution
      val serialModels = models.seq
      val resultMap = serialModels.zipWithIndex.map {
	case (m, i) =>
	  val results = this.apply(m, accGen)
	  (i.toString, QueryResult(results))
      }.toMap
      return QueryResultSet(resultMap)
    }
  }

  /** Selects a QueryProcessor for the given query. */
  def apply(root: Feature): QueryResult[ScoredDocument] =
    this.apply(root, DefaultAccumulator[ScoredDocument]())

  def apply[T <: ScoredObject[T]](
    root: Feature,
    acc: Accumulator[T]): QueryResult[T] = {
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

    val proc =
      if (c.numNeedsPreparing > 0) {
        new SimpleProcessor(root, acc) with Preparer {
          override def run() : QueryResult[T] = {
            prepare()
            super.run()
          }
        }
      } else {
        new SimpleProcessor(root, acc)
      }
    proc.run()
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
sealed trait QueryProcessor {
  // Pull in static functions
  import QueryProcessor.{isDone,matches}

  type DebugHook =
  (ScoredDocument, Seq[Feature], Index, QueryProcessor) => Unit
}

/** Processors that handle one query at a time in isolation should
  * extend this trait. Processors may assume they will be run
  * independently.
  */
trait SingleQueryProcessor[T <: ScoredObject[T]] extends QueryProcessor {
  def run(): QueryResult[T]
}

/** Will process more than one query simultaneously. How it does that is
  * it's business. In other words, check the implementing classes.
  */
trait MultiQueryProcessor[T <: ScoredObject[T]] extends QueryProcessor {
  // The only abstract method - all configuration is done at
  // construction of the processor
  def run(): QueryResultSet[T]
}
