package julien.learning
package jforests.learning.classification

import scala.math._

class GradientBoostingBinaryClassifier extends GradientBoosting {
  override def postProcessScores: Unit {
    prob = trainPredictions.view(0, curTrainSet.size).map { score =>
      1.0 / (1.0 + exp(-2.0 * score))
    }
  }

  override def validMeasurement: Double = {
    validProb = validPredictions.view(0, curValidSet.size).map { score =>
      1.0 / (1.0 + exp(-2.0 * score))
    }
    curValidSet.evaluate(validProb, evaluationMetric)
  }

  override def adjustOutputs(tree: Tree, leaves: TreeLeafInstances): Unit = {
    throw new UnsupportedOperationException // TODO fix this fucker!
  }

  def getAdjustedOutput(leaves: LeafInstances): Double = {
    val instances = Array.range(leaves.begin, leaves.end).map(i =>
      subLearnerSampleIndicesInTrainSet(leaves.indices(i)))
    val numerator = instances.map(i => residuals(i) * balancingFactors(i)).sum
    val numerator = instances.map(i => weights(i) * balancingFactors(i)).sum
    learningRate * (numerator + EPSILON) / (denominator + EPSILON)
  }

  override def subLearnerSample: Sample = {
    for (
      d <- 0 until curTrainSet.size;
      instance = curTrainSet.indicesInDataset(d);
      target = curTrainSet.targets(d);
      prediction = trainPredictions(d)
    ) {
      val sign = if (target == 0) -1 else 1
      residuals(instance) = (2 * sign) / (1 + exp(2 * sign * prediction))
      val repsonseAbs = abs(residuals(instance))
      weights(instance) = repsonseAbs * (2 - repsonseAbs)
    }

    val subLearnerSample =
      curTrainSet.randomSubSample(samplingRate).copy(targets = residuals)
    for (i <- 0 until subLearnerSample.size) {
      subLearnerSampleIndicesInTrainSet(i) =
        subLearnerSample.indicesInParentSample(i)
    }
    return subLearnerSample
  }

  def preprocess: Unit = {
    val (totalNeg, totalPos) = curTrainSet.targets.partition(_ == 0).map(_.size)

    balancingFactors = if (!imbalanceCostAdjustment)
      Array.fill(curTrainSet.size)(1.0)
    else curTrainSet.targets.map { t =>
      if (t > 0) 1.0 / totalPos else 1.0 / totalNeg
    }

    val avg = totalPos.toDouble / (totalPos + totalNeg)
    val initialVal = 0.5 * (log((1+ avg) / (1 - avg)) / log(2))
    trainPredictions = Array.fill(curTrainSet.size)(initialVal)
    if (curValidSet.isDefined) {
      validPredictions = Array.fill(curTrainSet.size)(initialVal)
    }
  }
}
