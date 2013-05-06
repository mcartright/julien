package julien
package retrieval

//import java.util.PriorityQueue
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

    if (q.size >= limit) {
      // if (elem.score > q.peek.score) {
      if (elem.score > q.head.score) {
        //q.poll
        //q add elem
        q.dequeue
        q += elem
      }
    } else {
      //q add elem
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
      val so = q.dequeue // q.poll
      so.rank = rank
      so +=: b
      rank -= 1
    }
    b.result
  }

  override def isEmpty: Boolean = q.isEmpty
  override def head: T = q.head // q.peek
  override def tail: DefaultAccumulator[T] = {
    DefaultAccumulator(q.tail)
    // val other = new PriorityQueue(q)
    // other.poll // drop the head
    // DefaultAccumulator(other)
  }
  override def length: Int = q.size
  override def apply(idx: Int): T = {
    q(idx)
    // val it = q.iterator()
    // for (i <- 0 until idx) it.next
    // it.next
  }
}
