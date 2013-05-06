package julien
package learning
package linear

import scala.collection.mutable.{ListBuffer,ArrayBuffer}
import scala.math._

class RankBoost(samples: List[RankList], features: Array[Int])
    extends Ranker(samples, features) {
  def clone: Ranker = RankBoost()
  val name: String = "RankBoost"

  val wRankers = ListBuffer[WeakRanker]()
  val rWeight = ListBuffer[Double]()
  val correctSamples = samples.map(_.getCorrectRanking)
  val sweight = Array.ofDim[Double](samples.size)(rl.size)(rl.size)
  var totalCorrectPairs = 0.0
  for (
    rl <- correctSamples;
    j <- 0 until rl.size;
    k <- (j+1 until rl.size-1).reverse) {
    if (rl(j).label > rl(k).label) {
      sweight(i)(j)(k) = 1.0
      totalCorrectPairs += 1
    } else {
      sweight(i)(j)(k) = 0.0
    }
  }

  val potential = ArrayBuffer[Array[Double]]()
  for (s <- samples) potential += Array.ofDim[Double](s.size)
  if (nThreshold <= 0) {
    val count = samples.map(_.size).sum
    thresholds = Array.ofDim[Double](features.length, count)
    var c = 0
    for (
      i <- 0 until samples.size;
      rl = samples(i);
      j <- 0 until rl.size;
      k <- features.length) {
      thresholds(k)(c) = rl(j)(features(k))
      c +=1
    }
  } else {
    val fmax = Array.fill(features.length)(-1E6)
    val fmin = Array.fill(features.length)(1E6)
    for (
      i <- samples.size;
      rl <- samples(i);
      j <- 0 until rl.size;
      k <- 0 until features.length) {
      val f = rl(j)(features(k))
      fmax(k) = max(fmax(k), f)
      fmin(k) = min(fmin(k), f)
    }
    thresholds = Array.ofDim[Double](features.length, nThreshold)
    for (i <- 0 until features.length) {
      val step = abs(fmax(i) - fmin(i))/nThreshold
      thresholds(i)(0) = fmax(i)
      for (j <- 1 until nThreshold) thresholds(i)(j) = thresholds(i)(j-1) - step
      thresholds(i)(nThreshold) = fmin(i) - 1.0E8
    }

    val tSortedIdx = thresholds.map(t => Sorter.sort(t, false))
    for (i <- 0 until features.length)
      sortedSamples += samples.map(s => reorder(s, features(i)))
  }

  def reorder(rl: RankList, fid: Int) : Array[Int] = {
    val scores = rl.points.map(p => p(fid))
    Sorter.sort(scores, false)
  }

  def updatePotential() {
    for (i <- 0 until samples.size; rl = samples(i); j <- 0 until rl.size) {
      var p = 0.0
      for (k <- j+1 until rl.size) p += sweight(i)(j)(k)
      for (k <- 0 until j) p -= sweight(i)(k)(j)
      potential(i)(j) = p
    }
  }

  def learnWeakRanker(): WeakRanker {
    for (i <- 0 until features.length) {
      val sSortedIndex = sortedSamples(i)
      val idx = tSortedIndex(i)
      val last = Array.fill(samples.size)(-1)
      r = 0.0
      for (j <- 0 until idx.length; t = thresholds(i)(idx(j))) {
        for (k <- 0 until samples.size; rl = samples(k); sk = sSortedIndex(k)) {
          for (l <- last(k)+1 until rl.size; p = rl(sk(l))) {
            if (p(features(i)) > t) {
              r += potential(k)(sk(l))
              last(k) = l
            }
          }
        }
        if (r > maxR) {
          maxR = r
          bestThreshold = t
          bestFid = features(i)
        }
      }
    }
    if (bestFid == -1) return null
    R_t = Z_t * maxR
    return new WeakRanker(bestFid, bestThreshold)
  }

  def eval(p: DataPoint): Double =
    wRankers.zipWithIndex.map((r, i) => rWeight(i) * r(i).score(p)).sum

  def learn(): Unit {
    for (t <- 1 until nIter) {
      updatePotential()
      val wr = learnWeakRanker()
      if (wr == null) return

      val alpha_t = 0.5 * ln((Z_t+R_t)/(Z_t-R_t))
      wRankers += wr
      rWeight += alpha_t
      Z_t = 0.0
      for (i <- 0 until samples.size; rl = samples(i)) {
        val D_t = Array.ofDim[Double](rl.size, rl.size)
        for (j <- 0 until rl.size-1) {
          for (k <- j+1 until rl.size) {
            D_t(j)(k) = sweight(i)(j)(k) *
            exp(alpha_t * (wr.score(rl(k)) - wr.score(rl(j))))
            Z_t += D_t(j)(k)
          }
          sweight(i) = D_t
        }
      }

      if (t % 1 == 0) {
        if (validationSamples != null) {
          val score = scorer.score(rank(validationSamples))
          if (score > bestScoreOnValidationData) {
            bestScoreOnValidationData = score
            bestModelRankers.clear
            bestModelRankers.addAll(wRankers)
            bestModelWeights.clear
            bestModelWeights.addAll(rWeight)
          }
        }
      }

      for (i <- 0 until samples.size; rl = samples(i)) {
        for (j <- 0 until rl.size; k <- j+1 until rl.size) {
          sweight(i)(j)(k) /= Z_t
        }
      }
    }

    if (validationSamples != null && !bestModelRankers.isEmpty) {
      wRankers.clear
      rWeight.clear
      wRankers.addAll(bestModelRankers)
      rWeight.addAll(bestModelWeights)
    }

    // TODO: Should probably return the trained ranker here
  }

  class WeakRanker(val fid: Int, val threshold: Double) {
    def score(p: DataPoint): Int = if (p(fid) > threshold) 1 else 0
  }
}
