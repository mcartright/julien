package julien
package retrieval

import scala.collection.BufferedIterator
import galago.core.util.ExtentArray

object Positions {
  def apply(e: ExtentArray) = new Positions(e)
}

class Positions(e: ExtentArray)
    extends BufferedIterator[Int] {
  private var index = 0

  override def length: Int = e.size
  override def head: Int = e.begins(index)
  def reset = index = 0
  def next: Int = {
    val toReturn = e.begins(index)
    index += 1
    toReturn
  }
  def hasNext: Boolean = index < e.size-1
}
