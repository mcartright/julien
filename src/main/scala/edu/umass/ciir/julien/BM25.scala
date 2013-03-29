package edu.umass.ciir.julien

class BM25(src: KeyedSource, b: Double = 0.75, k: Double = 1.2)
    extends Feature {
  val avgDocLength =
    src.collectionCount.toDouble / src.numDocuments.toDouble
  val idf = src.inverseDocFreq

  def calculate : Double = {
    val num = src.count + (k + 1)
    val den = src.count + (k * (1 - b + (b * src.length / avgDocLength)))
    idf * num / den
  }
}
