package julien
package learning
package neuralnet

class ListNet(samples: List[RankList], features: Array[Int])
    extends RankNet(samples, features) {
  def clone: Ranker = new ListNet
  val name: String = "ListNet"

  override def learn: Unit = {
    for (i <- 1 until nIter; j <- 0 until samples.size) {
      val labels = feedForward(samples(lj))
      backProp(labels)
      clearNeuronOutputs
    }
  }

  def backProp(labels: Array[Float]) {
    val p = PropParam(labels)
    outputLayer.computeDelta(p)
    outputLayer.updateWeight(p)
  }

  def feedForward(rl: RankList): Array[Float] = {
    val b = Array.newBuilder[Float]
    for (i <- 0 until rl.size) {
      addInput(rl(i))
      propagate(i)
      b += rl(i).label
    }
    return b.result
  }

  def estimateLoss: Unit = {
    var error = 0.0
    for (rl <- samples) {
      val scores = rl.map(p => eval(p))
      val sumLabelExp = rl.map(p => exp(p.label)).sum
      val sumScoreExp = scores.map(v => exp(v)).sum
      val err = rl.zipWithIndex.map { (p, i) =>
        -(exp(p.label)/sumLabelExp) * log2(exp(scores(i))/sumScoreExp)
      }.sum
      error += err/rl.size
    }
    lastError = error
  }
}
