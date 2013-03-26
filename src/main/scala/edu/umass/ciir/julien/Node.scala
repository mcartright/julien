package edu.umass.ciir.julien

import scala.collection.immutable.HashMap

import TermScorers.CountScorer

/** Describes the types of Nodes that are available in the
  * graph. Build it by example.
  */
type ScoreCombiner = (Double, Double) => Double

object Node {
  def term(t: String) : Node = Term(t, dirichlet())
  def features(t: String, f: List[FeatureFunction],
    w: List[Double], scorer = dirichlet()) =
    Features(t,f,w,scorer)
}

abstract class Node
case class Term(text: String, scorer: CountScorer) extends Node
case class Features(text: String, features: List[FeatureFunction],
  weights: List[Double], scorer: CountScorer) extends Node
case class Combine(children: List[Node], combiner: ScoreCombiner) extends Node
case class Intersected(scorer: CountScorer, filter: Intersection,
  terms: List[Term]) extends Node
