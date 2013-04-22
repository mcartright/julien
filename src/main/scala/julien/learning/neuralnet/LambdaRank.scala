package julien
package learning
package neuralnet

import julien.learning._

class LambdaRank(samples: List[RankList], features: Array[Int])
    extends RankNet(samples, features) {
  val name: String = "LambdaRank"
  def clone: Ranker = new LambdaRank
  var targetValue: Float2D.empty

  override protected batchFeedForward(rl: RankList): Int2D = {
    pMap = Array.ofDim[Array[Int]](rl.size)
    targetValue Array.ofDim[Array[Float]](rl.size)
    for (i <- 0 until rl.size) {
      addInput(rl(i))
      propagate(i)
      val count = (for (
        j <- 0 until rl.size;
        if (rl(i).label != rl(j).label)
          ) yield 1).sum
      pMap(i) = Array.ofDim[Int](count)
      targetValue(i) = Array.ofDim[Float](count)
      var k = 0
      for (j <- 0 until rl.size) {
        if (rl(i).label != rl(j).label) {
          pMap(i)(k) = j
          targetValue(i)(k) = if (rl(i).label > rl(j).label) 1 else 0
          k += 1
        }
      }
    }
    return pairMap
  }

  override def batchBackProp(pMap: Int2D, pWeight: Float2D) {
    for (
      i <- 0 until pMap.length;
      p = PropParam(i, pMap, pWeight, targetValue)
    ) {
      outputLayer.computeDelta(p)
      for (j <- (1 until layers.size-2).reverse) layers(j).updateDelta(p)
      outputLayer.updateWeight(p)
      for (j <- (1 until layers.size-2).reverse) layers(j).updateWeight(p)
    }
  }

  override def internalReorder(rl: RankList): RankList = rank(rl)
  override def computePairWeight(pMap: Int2D, rl: RankList): Float2D = {
    val changes = scorer.swapChange(rl)
    val weights = Array.ofDim[Array[Float]](pMap.length)
    for (i <- 0 until weights.length) {
      weights(i) = Array.ofDim[Float](pMap(i).length)
      for (j <- 0 until pMap(i).length) {
        val sign = if (rl(i).label > rl(pMap(i)(j)).label) 1 else -1
        weights(i)(j) = abs(changes(i)(pMap(i)(j)))*sign
      }
    }
    return weights
  }

  override def estimateLoss {
    var error = 0.0
    for (rl <- samples) {
      for (k <- 0 until rl.size-1; ol = eval(rl(k)); l <- k+1 until rl.size) {
        if (rl(k).label > rl(l).label) {
          val o2 = eval(rk(l))
          if (o1 < o2) misorderedPairs += 1
        }
      }
    }
    error = 1.0 - scoreOnTrainingData
    straightLoss = if (error > lastError) straightLoss + 1 else 0
    lastError = error
  }
}
