package edu.umass.ciir.julien

import QueryGraph._
import ExecutionGraph._
import Intersections._
import TermScorers._
import Node._

object ParameterizedScorer {
  def apply(
    features: List[FeatureFunction],
    weights: List[Double],
    scorer: CountScorer,
    l: MLI,
    it: TCI) = new FeatureScorer(features, weights, scorer, l, it)

  def apply(
    scorers: List[ParameterizedScorer],
    combiner: ScoreCombiner
  ) = new ComposedScorer(scorers, combiner)

  def apply(
    scorers: List[ParameterizedScorer],
    combiner: ScoreCombiner,
    weight: Double) : ComposedScorer = {
    val cs : ComposedScorer = this(scorers, combiner)
    cs.weight = weight
    cs
  }
  def apply(
    scorer: CountScorer,
    l: MLI,
    it: TCI) =
    new TermScorer(scorer, l, it)

  def apply(
    scorer: CountScorer,
    filter: Intersection,
    l : MLI,
    its : List[TEI]) =
    new IntersectedScorer(scorer, filter, l, its.toList)
}

abstract class ParameterizedScorer {
  private[this] var w : Any = 1.0
  def weight : Double = w match {
    case d: Double => d
    case f: FeatureFunction => f()
  }

  // Overload the weight property to allow for constants *and* functions
  def weight_= (in: () => Double) : Unit = w = in
  def weight_= (i: Int) : Unit = w = i
  def weight_= (d: Double): Unit = w = d

  def _score : Double
  def score = weight * _score
}

class ComposedScorer(
  scorers: List[ParameterizedScorer],
  combiner: ScoreCombiner
) extends ParameterizedScorer {
  def _score : Double = scorers.foldLeft(0.0)(combiner)
}

class TermScorer(
  scorer: CountScorer,
  l: MLI,
  it: TCI) extends ParameterizedScorer {
  def _score : Double = scorer(it.count, l.getCurrentLength)
}

// Delayed computation of a set of features combined with a weight vector
class FeatureScorer(
  features: List[FeatureFunction],
  weights: List[Double],
  scorer: CountScorer,
  l: MLI,
  it: TCI) extends TermScorer(scorer, l, it) {
  require(features.size == weights.size)

  weight = () => {
    val signals = features.map(f => f())
    val result = signals.zip(weights).map { case (s, w) =>
        s * w
    }.sum
    result
  }
}

class IntersectedScorer(
  scorer: CountScorer,
  filter: Intersection,
  l: MLI,
  its : List[TEI]) extends ParameterizedScorer {
  def _score : Double = {
    val extents = its.map(_.extents)
    scorer(filter(extents), l.getCurrentLength)
  }
}
