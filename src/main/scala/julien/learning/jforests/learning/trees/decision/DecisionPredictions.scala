package julien.learning
package jforests.learning.trees.decision

class DecisionPredictions(s: Sample, val numClasses: Int)
    extends Predictions(s) {
  def allocate(maxNumInstances: Int) {
    // sets up empty arrays of maxNumInstanes * numClasses. Yay.
  }

  def update(tree: DecisionTree) {
    for (i <- 0 until sample.size) {
      perInstanceDistribution(i) =
        tree.getDistributionForInstance(sample.dataset,
          sample.indicesInDataset(i)).map(_ * tree.weight).sum
    }
  }

  def evaluate(metric: EvaluationMetric): Double = {
    val pid = perInstanceDistribution
    val perInstancePredictions = numClasses match {
      case 2 => pid.map(dist => dist(1) / (dist(0) + dist(1)))
      case _ => pid.map(_.zipWithIndex.maxBy(_._1)._2)
    }
    return sample.evaluate(perInstancePredictions, metric)
  }

  def reset: Unit = {
    // zeroes out an existing array.
  }
}
