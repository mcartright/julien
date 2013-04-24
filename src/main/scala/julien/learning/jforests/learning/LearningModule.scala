package julien.learning
package jforests.learning

import scala.collection.mutable.Publisher


abstract class LearningModule(var name: String)
    extends Publisher[Event] {
  var treeWeight = 1.0
  var level = 1

  // For learner module stacking
  private[this] var pLearner: Option[LearningModule] = None
  private[this] var cLearner: Option[LearningModule] = None
  def parent_=(m: LearningModule) = parent = Some(m)
  def parent = pLearner
  def child_=(m: LeanringModule) = {
    sub = Some(m)
    m.parent = this
    m.level = this.level + 1
  }
  def child = cLearner

  // Abstract or default methods
  def learn(trainSet: Sample, validSet: Sample): Ensemble
  def validationMeasurement: Double
  def postProcess(tree: Tree, leaves: TreeLeafInstances): Unit = { /* no op */ }

}
