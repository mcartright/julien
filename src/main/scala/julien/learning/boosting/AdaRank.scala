package julien
package learning
package boosting

import scala.math._
import scala.collection.mutable.HashSet


object AdaRank {
  val nIter = 500
  val tolerance = 0.002
  val trainWithEnqueue = true
  val maxSelCount = 5
}

case class Sample(rl: RankList, weight: Double)

class AdaRank(samples: List[RankList], features: Array[Int])
    extends Ranker(samples, features) {
  import AdaRank._
  def eval(p: DataPoint): Double = rankers.map(r => r.weight * p(r.fid)).sum
  def clone: Ranker = AdaRank()
  def name: String = "AdaRank"

  // init
  val usedFeatures = HashSet[Int]()
  val wsamples = samples.map(rl => Sample(rl, 1.0f/samples.size))
  val backupSampleWeight = Array.ofDim[Double](wsamples.size)
  var lastTrainedScore = -1.0

  def learnWeakRanker: WeakRanker = {
    val availableFeatures =
      features.
        filter(i => fQueue.contains(i)).
        filter(i => usedFeatures.contains(i))
    val rankers = availableFeatures.map(i => WeakRanker(i))
    val candidates = rankers.map { r =>
      val score = wsamples.foldLeft(0.0) { (total, samp) =>
        val dp = scorer.score(r.rank(samp.rl) * samp.weight)
        total + dp
      }
      (score, r)
    }
    return candidates.maxBy(_._1)._2
  }

  def updateBestModelOnValidation() {
    bestModelRankers.clear
    bestModelRankers.addAll(rankers)
  }

  def learn() {
    if (!trainWithEnqueue) learn(1, false)
    else {
      var t = learn(1, true)
      // TODO: Don't get this...
      for (i <- (0 to featureQueue.size).reverse) {
        featureQueue.remove(i)
        t = learn(t, false) // TODO: Seriously, what the hell is this?
      }
    }

    if (validationSamples != null && !bestModelRankers.isEmpty) {
      rankers.clear
      rankers.addAll(bestModelRankers)
    }

    // TODO: This should probably return a "trained" ranker...
  }

  def learn(startIter: Int, withEnqueue: Boolean): Int = {
    for (t <- startIter to nIter) {
      val bestWR = learnWeakRanker()

      if (bestWR == null) return t // can this happen?

      if (withEnqueue) {
        if (bestWR.fid == lastFeature) {
          featureQueue.add(lastFeature)
          rankers = rankers.init
          backupSampleWeight.copyToArray(sweight)
          bestScoreOnValidationData = 0.0
          lastTrainedScore = backupTrainScore
          // TODO: was a continue here. Not supported in scala. Need to fix.
        } else {
          lastFeature = bestWR.fid
          sweight.copyToArray(backupSampleWeight)
          backupTrainScore = lastTrainedScore
        }
      }

      var tmps = wsamples.map(ws => scorer.score(bestWR.rank(ws.rl)))
      val num = tmps.zipWithIndex.map((t, idx) => wsamples(idx).weight * t).sum
      val denom =
        tmps.zipWithIndex.map((t, idx) => wsamples(idx).weight * t).sum
      rankers += bestWR
      val alpha_t = 0.5 * ln(num/denom)
      rankers.last.weight = alpha_t
      tmps = wsamples.map(s => scorer.score(rank(s.rl)))
      val total = tmps.map(t => exp(-alpha_t * t)).sum
      val trainedScore = tmps.sum / wsamples.size
      val delta = trainedScore + tolerance - lastTrainedScore
      if (!withEnqueue) {
        if (trainedScore != lastTrainedScore) {
          performanceChanged = true
          lastFeatureConsecutiveCount = 0
          usedFeatures.clear
        } else {
          performanceChanged = false
          if (lastFeature == bestWR.fid) {
            lastFeatureConsecutiveCount += 1
            if (lastFeatureConsecutiveCount == maxSelCount) {
              lastFeatureConsecutiveCount = 0
              usedFeatures.add(lastFeature)
            }
          } else {
            lastFeatureConsecutiveCount = 0
            usedFeatures.clear
          }
        }
        lastFeature = bestWR.fid
      }
      if (t % 1 == 0 && validationSamples != null) {
        val scoreOnValidation = scorer.score(rank(validationSamples))
        if (scoreOnValidation > bestScoreOnValidationData) {
          bestScoreOnValidationData = scoreOnValidation
          updateBestModelOnValidation()
        }
      }

      if (delta <= 0) {
        rankers = rankers.init
        // TODO: Had a break here. Need to fix.
      }

      lastTrainedScore = trainedScore
      for (ws <- wsamples)
        ws.weight *= exp(-alpha_t * scorer.score(rank(ws.rl)))/total
    }
    return t
  }
}
