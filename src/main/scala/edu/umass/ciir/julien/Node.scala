package edu.umass.ciir.julien

import scala.collection.immutable.HashMap

import TermScorers.CountScorer
import Intersections.Intersection

/** Describes the types of Nodes that are available in the
  * graph. Build it by example.
  */

object Node {
  type FeatureFunction = () => Double
  type ScoreCombiner = (Double, ParameterizedScorer) => Double
}

import Node._

abstract class Node
abstract class ScoreNode(val scorer: CountScorer) extends Node
class Term(val text: String, override val scorer: CountScorer)
    extends ScoreNode(scorer)
case class Features(override val text: String, features: List[FeatureFunction],
  weights: List[Double], override val scorer: CountScorer)
    extends Term(text, scorer)
case class Combine(children: List[Node], combiner: ScoreCombiner)
    extends Node
case class Intersected(override val scorer: CountScorer, filter: Intersection,
  children: List[Term]) extends ScoreNode(scorer)
