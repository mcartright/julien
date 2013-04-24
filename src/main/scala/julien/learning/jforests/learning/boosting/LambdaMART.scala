package julien.learning
package jforests
package learning.boosting

import scala.math._

class LambdaMART extends GradientBoosting("LambdaMART") {
  type CostFunction = (Double, Double) => Double

  override def postProcessScores: Unit = { }
  override def setSubLearnerSampleWeights(sample: RankingSample) = { }
  val sigmoidCache: Array[Double] = initSigmoids()

  def adjustedOutput(leaves: LeafInstances): Double = {
    val instances = Array.range(leaves.begin, leaves.end).map(i =>
      subLearnerSampleIndicesInTrainSet(leaves.indices(i)))
    val numerator = instances.map(i => residuals(i)).sum
    val denominator = instances.map(i => denomWeights(i)).sum
    (numerator + EPSILON) / (denominator + EPSILON)
  }

  def adjustOutputs(tree: Tree, leaves: TreeLeafInstances) {
    // TODO : REFACTOR THIS FUCKER
  }

  def subLearnerSample: Sample = {
    residuals = Array.range(0, curTrainSet.size)
    denomWeights = Array.range(0, curTrainSet.size)
    val ts = curTrainSet
    val ordering = MappedOrdering(trainPredictions, ts.targets)
    for (query <- 0 until trainSample.numQueries) {
      val begin = ts.queryBoundaries(query)

      // Make views for this iteration
      ordering.offset = begin
      val rview = residuals.view(begin, residuals.length)
      val dview = denomWeights.view(begin, denomWeights.length)
      val tview = trainPredictions.view(begin, trainPredictions.length)

      val numDocs = ts.queryBoundaries(query + 1) - begin
      val qMaxDCG = maxDCG(ts.queryIndices(query))
      val labels = targets.view(begin, begin+numDocs).map(_.toInt)

      // Permute order based on scores and labels
      val permutations = Array.range(0, numDocs).sorted(ordering)

      // And do our updates
      for (i <- 0 until numDocs; bidx = permutations(i); if labels(bidx) > 0) {
        for (j < 0 until numDocs; if i != j; widx = permutations(j)) {
          if (labels(bidx) > labels(widx)) {
            val scoreDiff = tview(bidx) - tview(widx)
            val rho = if (scoreDiff <= minScore) sigmoidCache.head
            else if (scoreDiff >= maxScore) sigmoidCache.last
            else sigmoidCache(((scoreDiff - minScore)/binWidth).toInt)

            val pairWeight =
              (NDCG.GAINS(labels(bidx)) - NDCG.GAINS(labels(widx))) *
            abs(NDCG.discounts(i) - NDCG.discounts(j)) / qMaxDCG

            rview(bidx) += rho * pairWeight
            rview(widx) -= rho * pairWeight

            val delta = rho * (1.0 - rho) * pairWeight
            dview(bidx) += delta
            dview(widx) += delta
          }
        }
      }
    }

    val newSample = trainSample.copy(targets = residuals)
    setSubLearnerSampleWeights(newSample) // does nothing for now.
    val unfilteredSample = trainSample.copy
    val subLearnerSample = unfilteredSample.randomSubSample(samplingRate)
    for (i <- 0 until) {
      subLearnerSampleIndicesInTrainSet(i) =
        unfilteredSample.
          indicesInParentSample(subLearnerSample.indicesInParentSample(i))
    }
    return subLearnerSample
  }

  def initSigmoids(
    numBins: Int,
    sigmoidParam: Double,
    fn: CostFunction): Array[Double] = {

    val min = MIN_EXP_POWER / sigmoidParam
    val max = -min
    val binWidth = (max - min) / numBins
    val scores = Array.range(0, numBins).map(i => minScore + (i*binWidth))
    return scores.map(s => fn(s))
  }

  // Cost functions (so far)
  def crossEntropy(score: Double, param: Double): Double = if (d > 0.0)
    1.0 - 1.0 / (1.0 + exp(-param * score))
  else
    1.0 / (1.0 + exp(param * score))

  def fidelity(score: Double, param: Double): Double = if (d > 0.0) {
    val e = exp(-2 * param * score)
    (-param / 2) * sqrt(e / pow(1 + e, 3))
  } else {
    val e = exp(param * score)
    (-param / 2) * sqrt(e / pow(1 + e, 3))
  }

  // Orderings for permuting
  abstract class MappedOrdering(s: Array[Double], l: Array[Double])
      extends Ordering[Int] {
    var scores = s
    var labels = l
    def offset_=(i: Int) {
      scores = s.view(offset, s.length)
      labels = l.view(offset, l.length)
    }
  }

  class ScoreLabelOrdering(s: Array[Double], l: Array[Double])
      extends MappedOrdering(s,l) {
    def compare(i1: Int, i2: Int): Int = {
      var result = scores(i2) compare scores(i1)  // bigger = better
      if (result != 0) return result
      return labels(i1) compare labels(i2) // smaller = better
    }
  }

  class ScoreRandomOrdering(s: Array[Double], l: Array[Double])
      extends MappedOrdering(s,l) {
    def compare(i1: Int, i2: Int): Int = {
      var result = scores(i2) compare scores(i1)  // bigger = better
      if (result != 0) return result
      if (Random.nextBoolean) -1 else 1
    }
  }

  class ScorePositionOrdering(s: Array[Double], l: Array[Double])
      extends MappedOrdering(s,l) {
    def compare(i1: Int, i2: Int): Int = {
      var result = scores(i2) compare scores(i1)  // bigger = better
      if (result != 0) return result
      i1 - i2
    }
  }
}
