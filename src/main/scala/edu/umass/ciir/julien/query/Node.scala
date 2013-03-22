import scala.collection.immutable.HashMap

/** Describes the types of Nodes that are available in the
  * graph.
  */
abstract class Node(parameters: Map[String, Any])
case class InteriorNode(
  parameters: Map[String, Any] = HashMap[String, Any](),
  children: List[Node] = List())
abstract class LeafNode(parameters: Map[String, Any]) extends Node(parameters)
case class KeyNode(parameters: Map[String, Any] = HashMap[String, Any]())
    extends LeafNode(parameters)
