package operators

import scala.collection.mutable.{Buffer,BufferLike,Builder,ResizableArray}
import scala.collection._
import scala.collection.generic._
import org.lemurproject.galago.core.util.ExtentArray

/** Represents a set of locations in a document
  * corresponding to where a particular count-based
  * operator occurs.
  * Replaces the ExtentArray class from Galago.
  */
class Positions(initialSize: Int)
    extends Buffer[Int]
    with BufferLike[Int, Positions]
    with IndexedSeqOptimized[Int, Positions]
    with Builder[Int, Positions]
    with ResizableArray[Int]
    with Value {
  def this() = this(16) // same as ArrayBuffer
}
object Positions {
  def apply() = new Positions(16)
  def apply(initialSize: Int) = new Positions(initialSize)
  def apply(e: ExtentArray) = {
    val pos = new Positions()
    for (i <- 0 until e.size) pos.append(e.begin(i))
    pos
  }
  implicit def canBuildFrom: CanBuildFrom[Positions, Int, Positions] =
    new CanBuildFrom[Positions, Int, Positions] {
      def apply(): Builder[Int, Positions] = newBuilder
      def apply(from: Positions): Builder[Int, Positions] = newBuilder
    }
  def newBuilder[Int]: Builder[Int, Positions] =
    new Positions().asInstanceOf[Builder[Int, Positions]]
}
