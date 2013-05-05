package julien
package retrieval

import scala.collection.mutable.PriorityQueue

object DefaultAccumulator {
  val defaultSize: Int = 1000

  def apply[T <: ScoredObject[T]](size: Int): DefaultAccumulator[T] =
    new DefaultAccumulator[T](new PriorityQueue[T](), size)
  def apply[T <: ScoredObject[T]](): DefaultAccumulator[T] =
    new DefaultAccumulator[T](new PriorityQueue[T](), defaultSize)

  /** We assume the provided queue has an ordering of "least is first". */
  def apply[T <: ScoredObject[T]](
    q: PriorityQueue[T],
    l: Int = defaultSize): DefaultAccumulator[T] =
    new DefaultAccumulator[T](q, l)
}

/** A standard implementation of an accumulator,
  * which is basically a PriorityQueue wrapper. More complicated
  * Accumulator structures will be based on this design, but this serves
  * a simple template.
  */
class DefaultAccumulator[T <: ScoredObject[T]] private(
  private[this] val q: PriorityQueue[T], val limit: Int
) extends Accumulator[T] {
  override val hasLimit: Boolean = true
  override def atCapacity: Boolean = q.size >= limit

  override def clear: Unit = q.clear
  override def +=(elem: T): this.type = {

//    q += elem
//    if (q.size > limit) q.dequeue
//    this

    if (q.size >= limit) {
      if (elem.score > q.head.score) {
        q.dequeue
        q += elem
      }
    } else {
      q += elem
    }

    this
  }

  /** Produces a List of the result type. Notices that this consumes
    * the contents of the Accumulator.
    */
  override def result(): List[T] = {
    val b = scala.collection.mutable.ListBuffer[T]()
    // building it back-to-front (i.e. a series of prepends)
    var rank = q.size
    while (!q.isEmpty) {
      val so = q.dequeue
      so.rank = rank
      so +=: b
      rank -= 1
    }
    b.result
  }

  override def isEmpty: Boolean = q.isEmpty
  override def head: T = q.head
  override def tail: DefaultAccumulator[T] = DefaultAccumulator(q.tail)
  override def length: Int = q.length
  override def apply(idx: Int): T = q(idx)

}
