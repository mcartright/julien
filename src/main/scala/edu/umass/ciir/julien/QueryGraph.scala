package edu.umass.ciir.julien

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.{HashSet,ListBuffer}

import Intersections._
import TermScorers._

/** An abstract graph of a query that describes the WHAT of a retrieval model,
  * but it does not describe the HOW. That is handled when 'prepare' is called
  * and this graph is copied into an ExecutionGraph object, which contains all
  * the details necessary to actually score the model specified by the graph.
  */
class QueryGraph() {
  var nodes = List[Node]()
  val indexes = HashSet[Symbol]()
  var index: Option[Symbol] = None

  def addIndex(name: Symbol) : Unit = {
    indexes.add(name)
    if (index.isEmpty) index = Some(name)
  }
  def add(n : Node) : Unit = nodes = n :: nodes
  def add(ns : List[Node]) : Unit = nodes = nodes ++ ns

  def leaves : List[Term] = flatten.filter {
    _.isInstanceOf[Term]
  }.asInstanceOf[List[Term]]

  def scoreNodes : List[ScoreNode] = flatten.filter {
    _.isInstanceOf[ScoreNode]
  }.asInstanceOf[List[ScoreNode]]

  def flatten() : List[Node] = {
    val flatList = ListBuffer[Node]()
    walk(flatList.append(_))
    flatList.toList
  }

  type NodeFunction = (Node) => Unit
  def walk(f: NodeFunction) : Unit = _walk(nodes, f)
  def _walk(localNodes: List[Node], f: NodeFunction) : Unit = {
    for (n <- localNodes) {
      f(n) // pre-visit walk for now
      n match {
        case Intersected(scorer, filter, children) => _walk(children, f)
        case Combine(children, combiner) => _walk(children, f)
      }
    }
  }
}

object QueryGraph {
  def apply() = new QueryGraph
}
