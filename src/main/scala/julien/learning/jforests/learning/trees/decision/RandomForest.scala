package julien.learning
package jforests
package learning.trees.decision

class RandomForest extends Bagging {
  val treeLearner = DecisionTreeLearner()
  setSubModule(treeLearner)

  def newPredictions: Predictions = new DecisionPredictions(numClasses)
}
