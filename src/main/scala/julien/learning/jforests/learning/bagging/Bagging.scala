package julien
package learning
package jforests
package bagging

abstract class Bagging extends LearningModule("bagging") {
  def getNewPredictions: Predictions

  def learn(trainSet: Sample, validSet: Sample): Ensemble = {
    val ensemble = Ensemble()
    subLearner.treeWeight = treeWeight / bagCount
    for (iter <- 1 to bagCount) {
      val subLearnerTrainSet =
        trainSet.randomSubSample(baggingTrainFraction)
      val subLearnerOutOfTrainSet =
        trainSet.outOfSample(sunbLearnerTrainSet)
      val subLearnerValidSet = if (validSet == null || validSet.isEmpty)
        subLearnerOutOfTrainSet
      else
        validSet
      val subEnsemble = subLearner.learn(subLearnerTrainSet, subLearnerValidSet)

      for ((tree, i) <- subEnsemble.zipWithIndex) {
        if (backfit) tree.backfit(subLearnOutOfTrainSet)
        ensemble += tree
      }

      if (!validSet.isEmpty) {
        for (tree <- subEnsemble) {
          validPredictions.update(tree, 1.0 / bagCount)
        }
        lastValidMeasurement = validPredictions.evaluate(evaluationMetric)
      }
      onIterationEnd
    }
    onLearningEnd
    return ensemble
  }

  def getValidationMeasurement: Double = lastValidMeasurement
  def postProcess(tree: Tree, leaves: TreeLeafInstances): Unit =
    if (parentLearner != null) parentLearner.postProcess(tree, leaves)
  }
}
