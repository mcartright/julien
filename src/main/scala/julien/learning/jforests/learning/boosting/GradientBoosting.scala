package julien
package learning
package jforests
package boosting

class GradientBoosting extends LearningModule("Gradient Boosting") {
  def learn(trainSet: Sample, validSet: Sample): Ensemble = {
    val ensemble = Ensemble
    bestValidationMeasurement = Double.NaN
    var earlyStoppingIteration, bestIteration = 0
    val treeCounts = Array.ofDim[Int](numSubModules)
    subLearner.treeWeight = treeWeight
    for (curIter <- 1 until numSubModules) {
      val subLearnerSample = getSubLearnerSample
      val subEnsemble = subLearner.learn(subLearner, validSet)
      if (subEnsemble == null) {
        // TODO: was break. Fix.
      }

      for ((tree, i) <- subEnsemble.zipWithIndex) {
        ensemble += tree
        if (validSet.isDefined) tree.asInstanceOf[RegressionTree].
          updateScores(validSet, validPredictions, 1.0)
        }
      }
      treeCounts(curIter-1) = ensemble.numTrees

      if (validSet == null) earlyStoppingIteration = curIter
      else {
        val validMeasurement = getValidMeasurement
        if (evaluationMetric.isFirstBetter(
          validMeasurement,
          bestValidationMeasurement,
          earlyStoppingTolerance)) {
          earlyStoppingIteration = curIteration
          if (evaluationMetric.isFirstBetter(
            validMeasurement,
            bestValidationMeasurement,
            0)) {
            bestValidationMeasurement = validMeasurement
            bestIteration = curIter
          }
        }

        if (curIter - bestIteration > 100) {
          // TODO : was break. Fix.
        }
      }
      onIterationEnd
    }

    if (earlyStoppingIteration > 0) {
      val keepers = treeCounts(earlyStoppingIteration-1)
      val deleters = ensemble.size - keepers
      ensemble = ensemble.dropRight(deleters)
    }
    onLearningEnd
    return ensemble
  }

  def getSubLearnerSample: Sample {
    val residuals =
      curTrainSet.targets.zip(trainPredictions).map((t, p) => t - p)
    val subLearnerSample = curTrainSet.clone
    subLearnerSample.targets = residuals
    subLearnerSample.getRandomSubSample(samplingRate)
  }

  def getValidMeasurement: Double =
    curValidSet.evaluate(validPredictions, evaluationMetric)

  def getTrainMeasurement: Double =
    curTrainSet.evaluate(trainPredictions, evaluationMetric)

  override def postProcessScores { /* no op */ }
  def postProcess(tree: Tree, leaves: TreeLeafInstances) {
    adjustOutputs(tree, leaves)
    LearningUtils.updateScores(
      curTrainSet,
      trainPredictions,
      tree.asInstanceOf[RegressionTree],
      1.0)
    postProcessScores
  }

  def adjustOutputs(tree: Tree, leaves: TreeLeafInstances): Unit =
    tree.asInstanceOf[RegressionTree].multiplyLeafOutputs(learningRate)
}
