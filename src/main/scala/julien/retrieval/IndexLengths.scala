package julien
package retrieval

import scala.concurrent.{Future,Await,future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import julien.galago.core.index.LengthsReader._

sealed abstract class IndexLengths(index: Index)
    extends LengthsView
    with ChildlessOp
    with Movable

object IndexLengths {
  def apply()(implicit index: Index) = memoryIfPossible(index, None)
  def apply(f: String)(implicit index: Index) = memoryIfPossible(index, Some(f))

  private val cache =
    scala.collection.mutable.HashMap[String, Future[Array[Int]]]()

  def memoryIfPossible(
    index: Index,
    wrappedField: Option[String]
  ): IndexLengths = {
    val field = wrappedField match {
      case Some(f) => f
      case None => index.defaultPart
    }
    // short-circuit
    if (cache.contains(field)) return new ArrayLengths(index, cache(field))

    // Actually do the work
    val availableMemory = Runtime.getRuntime.freeMemory
    val iterator = index.lengthsIterator(field)
    val size = iterator.sizeInBytes
    if (availableMemory > size * 1.3) {
      val fut = future { scanIntoMemory(iterator) }
      cache(field) = fut
      new ArrayLengths(index, fut)
    } else {
      new StreamLengths(index, iterator)
    }
  }

  def scanIntoMemory(l: LengthsIterator): Array[Int] = {
    val buffer = Array.newBuilder[Int]
    var i = 0
    while (!l.isDone) {
      l.syncTo(i)
      buffer += l.getCurrentLength
      i += 1
    }
    buffer.result
  }
}

final class StreamLengths(i: Index, li: LengthsIterator)
    extends IndexLengths(i)
    with IteratedHook[LengthsIterator] {
  underlying = li
  override def toString = s"lengths:" + index.toString
  def length: Int = underlying.getCurrentLength
  override def isDense: Boolean = true
  def getIterator(i: Index): LengthsIterator = underlying
}

final class ArrayLengths(i: Index, nascentArray: Future[Array[Int]])
    extends IndexLengths(i)
    with Movable {
  private var current: Int = 0
  def isDense: Boolean = true

  // This will block to set this array if the read operation isn't complete,
  // but it will only block when it's asked for (hence the lazy)
  // I'm only hoping that if the Future is already done, that this doesn't
  // somehow cause the Await to block forever
  // (it should instead return immediately)
  lazy val lengthArray: Array[Int] = {
    Await.result(nascentArray, Duration.Inf) // arbitrary
  }

  def reset: Unit = current = 0
  def length: Int = lengthArray(current)
  def isDone: Boolean = current >= lengthArray.length - 1
  def at: Int = current
  def moveTo(id: Int) = current = id
  def movePast(id: Int) = current = id+1
  override def size: Int = lengthArray.length
}
