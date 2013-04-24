package julien
package learning
package jforests
package sample

import scala.util.Random

object Sample {
  def apply(d: Dataset): Sample = {
    val sz = d.size
    val filler = Array.range(0, sz)
    new Sample(d, filler, filler, dataset.targets, Array.empty, d.size)
  }

  def apply(s: Sample) = new Sample(
    s.dataset,
    s.indicesInDataset,
    s.weights,
    s.targets,
    s.indicesInParentSample,
    s.size)

  def apply(
    d: Dataset,
    iid: Array[Int],
    w: Array[Double],
    t: Array[Double],
    iips: Array[Int],
    s: Int
  ): Sample = new Sample(d, iid, w, t, iips, s)
}

class Sample private (
  val dataset: Dataset,
  val indicesInDataset: Array[Int],
  val weights: Array[Double],
  val targets: Array[Double],
  val indicesInParentSample: Array[Int],
  val size: Int
) {
  lazy val isEmpty: Boolean = size == 0
  def getRandomSubSample(rate: Double): Sample {
    assume(rate >= 0.0 && rate <= 1.0, s"got weird sample rate.")
    if (rate == 1.0) {
      val subSamp = Sample(this)
      subSamp.indicesInParentSample = Array.range(0, size)
      return subSamp
    } else {
      val sampleSize = (size * rate).toInt
      val sampleIndices =
        Random.shuffle(Array.range(0, size)).take(sampleSize).sort
      // Map the large sample down to the subsample using the indices
     return Sample(
        dataset,
        sampleIndices.map(idx => indicesInDataset(idx)),
        sampleIndices.map(idx => weights(idx)),
        sampleIndices.map(idx => targets(idx)),
        sampleIndices,
        sampleSize)
    }
  }

  def evaluate(predictions: Array[Double], em: EvaluationMetric): Double =
    em.measure(predictions, this)

  def evaluate(
    predictions: Array[Double],
    em: EvaluationMetric,
    factor: Double): Double = em.measure(predictions.map(_ * factor), this)

  def getOutOfSample(subSample: Sample): Sample = {
    assume(this.dataset == subSample.dataset, s"need same dataset")
    val oosIndices = this.indicesInDataset.diff(subSample.indicesInDataset)
    Sample(
      dataset,
      oosIndices.map(idx => indicesInDataset(idx)),
      oosIndices.map(idx => weights(idx)),
      oosIndices.map(idx => targets(idx)),
      oosIndices,
      oosIndices.size)
  }
}
