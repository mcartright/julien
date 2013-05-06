package julien
package retrieval

import scala.collection.mutable.PriorityQueue

object ArrayAccumulator {
  val defaultSize: Int = 1000

  def apply[T <: ScoredObject[T]](indexSize:Int, numResults:Int): ArrayAccumulator[T] =
    new ArrayAccumulator[T](new Array[Double](indexSize), numResults)

  def apply[T <: ScoredObject[T]](indexSize:Int): ArrayAccumulator[T] =
    new ArrayAccumulator[T](new Array[Double](indexSize), defaultSize)

  /** We assume the provided queue has an ordering of "least is first". */
  def apply[T <: ScoredObject[T]](a: Array[Double], k: Int = defaultSize): ArrayAccumulator[T] =
    new ArrayAccumulator[T](a, k)
}

/** A standard implementation of an accumulator,
  * which is basically a PriorityQueue wrapper. More complicated
  * Accumulator structures will be based on this design, but this serves
  * a simple template.
  */
class ArrayAccumulator[T <: ScoredObject[T]] private(
  private[this] val acc: Array[Double],
  val limit: Int
  ) extends Accumulator[T] {

  override def clear: Unit = {
    doubleFill(acc, 0.0)
  }

  private final def doubleFill(array: Array[Double] , value:Double) {
    val len = array.length
    if (len > 0)
      array(0) = value
    var i = 1
    while (i < len) {
      val next = if ((len - i) < i)   {
        (len - i)
      }  else {
        i
      }
      System.arraycopy( array, 0, array, i, next)
      i += i
    }

  }

  override def +=(elem: T): this.type = {
    acc(elem.id) += elem.score
    this
  }

  /** Produces a List of the result type. Notices that this consumes
    * the contents of the Accumulator.
    */
  override def result(): List[T] = {

    val topK = new PriorityQueue[ScoredDocument]()

    var i = 0
    while (i < acc.length) {
      val curScore = acc(i)
      if (curScore > 0) {

        if (topK.size >= limit) {
          if (curScore > topK.head.score) {
            topK.dequeue
            topK += ScoredDocument(InternalId(i), curScore)
          }
        } else {
          topK  += ScoredDocument(InternalId(i), curScore)
        }
      }
      i += 1
    }


    val b = scala.collection.mutable.ListBuffer[T]()
    // building it back-to-front (i.e. a series of prepends)
    while (!topK.isEmpty) topK.dequeue.asInstanceOf[T] +=: b
    b.result
  }

  override def length: Int = acc.length
  override def apply(idx: Int): T = ScoredDocument(InternalId(idx), acc(idx)).asInstanceOf[T]


}
