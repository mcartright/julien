package edu.umass.ciir.julien

class JelinekMercer(src: KeyedSource, lambda: Double = 0.2)
    extends Feature {
  val cf = src.collectionCount.toDouble / src.collectionLength.toDouble

  def calculate : Double = {
    val foreground = src.count.toDouble / src.length
    scala.math.log((lambda*foreground) + ((1.0-lambda)*cf))
  }
}
