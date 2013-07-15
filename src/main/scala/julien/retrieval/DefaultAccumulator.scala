package julien
package retrieval

import scala.collection.mutable.PriorityQueue

object DefaultAccumulator {
  var defaultSize: Int = 5

  def apply[T <: ScoredObject](size: Int)
    (implicit order: Ordering[T]): DefaultAccumulator[T] =
    new DefaultAccumulator[T](new PriorityQueue[T]()(order), size)
  def apply[T <: ScoredObject]()
    (implicit order: Ordering[T]): DefaultAccumulator[T] =
    new DefaultAccumulator[T](new PriorityQueue[T]()(order), defaultSize)

  /** We assume the provided queue has an ordering of "least is first". */
  def apply[T <: ScoredObject](
    q: PriorityQueue[T],
    l: Int = defaultSize): DefaultAccumulator[T] =
    new DefaultAccumulator[T](q, l)
}

/** A standard implementation of an accumulator,
  * which is basically a PriorityQueue wrapper. More complicated
  * Accumulator structures will be based on this design, but this serves
  * a simple template.
  */
class DefaultAccumulator[T <: ScoredObject] private(
  private[this] val q: PriorityQueue[T], l: Int
) extends Accumulator[T] {
  override val limit: Option[Int] = Some(l)
  override def atCapacity: Boolean = q.size >= l

  override def clear: Unit = q.clear
  override def +=(elem: T): this.type = {
    if (q.size >= l) {
      if (elem.score > q.head.score) {
        q.dequeue
        q += elem
      }
    } else {
      q += elem
    }
    this
  }

  /** Produces a List of the result type. Notice that this consumes
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

  override def iterator: Iterator[T] = q.iterator
  override def isEmpty: Boolean = q.isEmpty
  override def head: T = q.head
  override def tail: DefaultAccumulator[T] = DefaultAccumulator(q.tail)
}
