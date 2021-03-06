package julien
package retrieval

import scala.collection.mutable.{ListBuffer,TreeSet}

object PercentWorseAccumulator {
  def apply[T <: ScoredObject](pct: Double)
    (implicit order: Ordering[T]): PercentWorseAccumulator[T] =
    new PercentWorseAccumulator(TreeSet[T]()(order), pct)
  def apply[T <: ScoredObject]()
    (implicit order: Ordering[T]): PercentWorseAccumulator[T] =
    new PercentWorseAccumulator(TreeSet[T]()(order), 0.10)
}

/** This accumulator operates by only keeping
  * [[julien.ScoredObject ScoredObjects]] within a
  * certain percentage of the maximum score in the accumulator.
  */
class PercentWorseAccumulator[T <: ScoredObject] private(
  private[this] val elements: TreeSet[T],
  val pct: Double
) extends Accumulator[T] {
  assume(pct > 0.0 && pct < 1.0, s"Expect pct in (0,1) range. Got $pct")
  override def clear: Unit = elements.clear
  override def +=(elem: T): this.type = {
    elements += elem
    pruneIfNeeded()
    this
  }

  override def result(): List[T] = {
    val b = ListBuffer[T]()
    for (e <- elements) e +=: b
    b.result
  }

  override def iterator: Iterator[T] = elements.iterator
  override def isEmpty: Boolean = elements.isEmpty
  override def head: T = elements.head
  override def tail: PercentWorseAccumulator[T] =
    new PercentWorseAccumulator(elements.tail, pct)

  private def pruneIfNeeded() {
    val max = elements.lastKey.score
    val allowable = max - (max * pct)
    while (!elements.isEmpty &&
      elements.head.score < allowable) {
      elements.remove(elements.head)
    }
  }
}
