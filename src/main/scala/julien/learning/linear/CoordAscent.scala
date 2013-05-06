package julien
package learning
package linear

import scala.math._

object CoordAscent {
  val slack = 0.001
  val regularized = false
  val tolerance = 0.001
  val stepScale = 2.0
  val stepBase = 0.05
  val nRestart = 2
  val nMaxIteration = 25
}

class CoordAscent(samples: List[RankList], features: Array[Int])
    extends Ranker(samples, features) {
  import CoordAscent._

  def clone: Ranker = new CoordAscent
  val name: String = "CoordAscent"

  val weights = Array.fill(features.length)(1.0).map(_ / features.length)
  var current_feature = -1
  var weight_change = -1.0

  // TODO: OMG PLEASE REFACTOR ME
  def learn: Unit = {
    val regVector = Array.range(0, weights.length).map(_ / weights.length)
    val sign = Array(1, -1)
    for (r <- 0 until nRestart) {
      var consecutive_fails = 0
      for (i <- 0 until features.length) weight(i) = 1.0f/features.length
      current_feature = -1
      val startScore = scorer.score(rank(samples))
      var bestScore = startScore
      var bestWeights = Array(weights)

      while (
        (weights.length > 1 && consecutive_fails < weights.length -1) ||
          (weights.length==1 && consecutive_fails == 0)
      ) {
        val fids = getShuffledFeatures
        for (i <- 0 until fids.length; origWeight = weights(fids(i))) {
          current_feature = fids(i)
          var totalStep, bestTotalStep = 0
          var succeeds = false
          for (s <- 0 until sign.length) {
            var step= 0.001 * sign(s)
            if (origWeight != 0.0 && abs(step) > 0.5 * abs(origWeight)) {
              step = stepBase * abs(origWeight)
              totalStep = step
              for (j <- 0 until nMaxIter; w = origWeight + totalStep) {
                weight_change = step
                weights(fids(i)) = w
                var score = scorer.score(rank(samples))
                if (regularized) {
                  val penalty = slack * getDistance(weight, regVector)
                  score -= penalty
                }

                if (score > bestScore) {
                  bestScore = score
                  bestTotalStep = totalStep
                  succeeds = true
                }

                if (j < nMaxIter-1) {
                  step *= stepScale
                  totalStep += step
                }
              }

              if (succeeds) {
                // TODO : this was a break. Need to refactor to fit it
              } else if (s < sign.length-1) {
                weight_change = -totalStep
                updateCached
                weights(fids(i)) = origWeight
              }
            }
          }

          if (succeeds) {
            weight_change = bestTotalStep - totalStep
            updateCached
            weight(fids(i)) = origWeight + bestTotalStep
            consecutive_fails = 0
            scaleCached(normalize(weights))
          } else {
            consecutive_fails += 1
            weight_change = -totalStep
            updateCached
            weights(fids(i)) = origWeight
          }
        }

        if (bestScore - startScore < tolerance) {
          // TODO: was a break. Have to fix
        }
      }

      if (bestModel == null || bestScore > bestModelScore) {
        bestModelScore = bestScore
        bestModel = bestWeight
      }
    }

    current_feature = -1
    scoreOnTrainingData = round(scorer.score(rank(samples)))
    // TODO: This SERIOUSLY needs to return something useful.
  }

  def rank(rl: RankList): RankList = {
    val score = Array.ofDim[Double](rl.size)
    if (current_feature == -1) {
      for (i <- 0 until rl.size; j <- 0 until features.length) {
        score(i) += weights(j) * rl(i)(features(j))
        rl(i).cache(score(i))
      }
    } else {
      for (i <- 0 until rl.size) {
        score(i) =
          rl(i).cached + weight_change * rl(i)(features(current_feature))
        rl(i).cache(score(i))
      }
    }
    val idx = Sorter.sort(score, false)
    return new RankList(rl, idx)
  }

  def eval(p: DataPoint): Double = features.zipWithIndex.map { (fid, i) =>
    weights(i) * p(fid)
  }.sum

  def updateCached: Unit = {
    for (rl <- samples; i <- 0 until rl.size) {
      val score =
        rl(i).cached + weight_change * rl(i)(features(current_feature))
      rl(i).cache(score)
    }
  }

  def scaleCached(sum: Double): Unit = {
    for (rl <- samples; i <- 0 until rl.size)
      rl(i).cache(rl(i).cached / sum)
  }

  def getShuffledFeatures: Array[Int] =
    scala.util.Random.shuffle(Array.range(0, features.length))

  def distance(ca: CoordAscent): Double = getDistance(weight, ca.weight)
  def getDistance(w1: Array[Double], w2: Array[Double]): Double = {
    assume(w1.length == w2.length)
    val s1 = w1.map(i => abs(i)).sum
    val s2 = w2.map(i => abs(i)).sum
    val dist = w1.zip(w2).map { (a,b) =>
      val t = a/s1 - b/s2
      t * t
    }.sum
    return sqrt(dist)
  }

  def normalize(weights: Array[Double]): Double = {
    val sum = weights.map(v => abs(v)).sum
    if (sum > 0) {
      for (i <- 0 until weights.length) weights(i) /= sum
      sum
    } else {
      for (i <- 0 until weights.length) weights(i) = 1.0/weights.length
      1
    }
  }
}
