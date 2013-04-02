package edu.umass.ciir.julien

class BM25(
  bs: BoundSource,
  cs: CollectionSource,
  b: Double = 0.75,
  k: Double = 1.2)
    extends Feature {
  val avgDocLength =
    bs.collectionCount.toDouble / cs.numDocuments.toDouble
  val idf = scala.math.log(cs.numDocuments / (bs.docFreq + 0.5))

  def calculate : Double = {
    val num = bs.count + (k + 1)
    val den = bs.count + (k * (1 - b + (b * bs.length / avgDocLength)))
    idf * num / den
  }
}
