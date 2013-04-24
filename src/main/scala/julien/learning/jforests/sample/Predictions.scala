package julien.learning
package jforests.sample

abstract class Predictions(var sample: Sample) {
  def allocate(maxNumValidInstances: Int): Unit
  def update(tree: Tree): Unit
  def evaluate(metric: EvaluationMetric): Double
  def reset: Unit
}
