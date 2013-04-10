package garage
package sources

class JelinekMercer(
  bs: BoundSource,
  cs: CollectionSource,
  lambda: Double = 0.2)
    extends Feature {
  val cf = bs.collectionCount.toDouble / cs.collectionLength.toDouble

  def calculate : Double = {
    val foreground = bs.count.toDouble / bs.length
    scala.math.log((lambda*foreground) + ((1.0-lambda)*cf))
  }
}
