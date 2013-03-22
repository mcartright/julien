package edu.umass.ciir.julien.query

import scala.collection.List


/** An abstract graph of a query that describes the WHAT of a retrieval model,
  * but it does not describe the HOW. That is handled when 'prepare' is called
  * and this graph is copied into an ExecutionGraph object, which contains all
  * the details necessary to actually score the model specified by the graph.
  */
class QueryGraph() {
  var topLevelNodes = List[Node]()
  var indexes = Map[Symbol, DiskIndex]()
  var defaultIndex : Option[Symbol] = None

  def addIndex(name: String, index: DiskIndex) : Unit = {
    indexes = indexes + (name, index)
    if (defaultIndex.isEmpty) {
      defaultIndex = Some(name)
    }
  }

  def add(n : Node) : Unit = topLevelNodes = n :: topLevelNodes
  def add(ns : List[Node]) : Unit = topLevelNodes = ns ::: topLevelNodes

  // Intersection generators
  def od(width: Int = 1) : Intersection =
    (extents: List[ExtentArray]) => {
      val iterators = extents.map(new ExtentArrayIterator(_))
      var count = 0
      while (iterators.forall(_.isDone == false)) {
        // Make sure an iterator is after its preceding one
        iterators.reduceLeft { (i1, i2) =>
          while (i2.currentBegin < i1.currentEnd && !i2.isDone) i2.next
          i2
        }
        // Now see if we have a valid match
        val matched = iterators.sliding(2,1).map { P =>
          // Map pairs of iterators to booleans. All true = have match
          P(1).currentBegin - P(0).currentEnd < width
        }.reduceLeft((A,B) => A && B)
        if (matched) count += 1
        iterators(0).next
      }
      count
    }

  def uw(width: Int = 1) : Intersection =
    (extents: List[ExtentArray]) => {
      val iterators = extents.map(new ExtentArrayIterator(_))
      var count = 0
      while (iterators.forall(_.isDone == false)) {
        // Find bounds
        val minPos = iterators.map(_.currentBegin).min
        val maxPos = iterators.map(_.currentEnd).max

        // see if it fits
        if (maxPos - minPos <= width || width == -1) count += 1

        // move all lower bound iterators foward
        for (it <- iterators; if (it.currentBegin == minPos)) it.next
      }
      count
    }

  // Scoring function generators
  def dirichlet(cf: Double, mu: Double = 1500) : CountScorer =
    (count:Int, length: Int) => {
      val num = count + (mu*cf)
      val den = length + mu
      scala.math.log(num / den)
    }

  def jm(cf: Double, lambda: Double = 0.2) : CountScorer =
    (count:Int, length: Int) => {
      val foreground = count.toDouble / length
      scala.math.log((lambda*foreground) + ((1.0-lambda)*cf))
    }

  def bm25(
    adl: Double,
    idf: Double,
    b: Double = 0.75,
    k: Double = 1.2) : CountScorer =
    (count: Int, length: Int) => {
      val num = count * (k + 1)
      val den = count + (k * (1 - b + (b * length / adl)))
      idf * num / den
    }
}
