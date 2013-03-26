package edu.umass.ciir.julien

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.HashSet

import Intersections._
import TermScorers._

/** An abstract graph of a query that describes the WHAT of a retrieval model,
  * but it does not describe the HOW. That is handled when 'prepare' is called
  * and this graph is copied into an ExecutionGraph object, which contains all
  * the details necessary to actually score the model specified by the graph.
  */
class QueryGraph() {
  var nodes : List[Nodes]
  val indexes = HashSet[Symbol]()

  def addIndex(name: Symbol) : Unit = indexes.add(name)
  def add(n : Node) : Unit = topLevelNodes = n :: nodes
  def add(ns : List[Node]) : Unit = nodes = ns ::: nodes

  def leaves : List[Term] = {
    List.empty
  }

  def scoreNodes : List[ScoreNode] = {
    List.empty
  }
}

object QueryGraph {
  def apply() = new QueryGraph
}
