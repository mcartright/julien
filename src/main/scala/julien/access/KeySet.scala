package julien
package access

import org.lemurproject.galago.core.index.KeyIterator
import org.lemurproject.galago.tupleflow.Utility
import scala.collection.SortedSet
import scala.collection.generic.SortedSetFactory

object KeySet {
  implicit lazy val ordering: Ordering[String] =
    new Ordering[String] {
      def compare(x: String, y: String): Int =
        Utility.compare(Utility.fromString(x), Utility.fromString(y))
    }

  def apply(g: () => KeyIterator) = new KeySet(g, None, None)
  def apply(g: () => KeyIterator, lo: String, hi: String) =
    new KeySet(g, Some(lo), Some(hi))
}

class KeySet private(
  iterGen:() => KeyIterator,
  lokey: Option[String],
  hikey: Option[String]) // specifically want a "generator"
    extends SortedSet[String] {
  private[this] val underlying = iterGen()

  def +(elem: String): KeySet =
    throw new UnsupportedOperationException("Not doing dynamic keys.")

  def -(elem: String): KeySet =
    throw new UnsupportedOperationException("Not doing dynamic keys.")

  // need to finish this
  def contains(elem: String): Boolean =
    (lokey.isEmpty ||
      Utility.compare(Utility.fromString(lokey.get), underlying.getKey) < 0) &&
  (hikey.isEmpty ||
    Utility.compare(Utility.fromString(lokey.get), underlying.getKey) > 0) &&
  underlying.findKey(Utility.fromString(elem))

  def iterator: Iterator[String] = new Iterator[String] {
    val other = iterGen()

    if (lokey.isDefined) other.skipToKey(Utility.fromString(lokey.get))

    def hasNext = !other.isDone &&
    (hikey.isEmpty ||
      Utility.compare(other.getKey, Utility.fromString(hikey.get)) < 0)

    def next: String = {
      val s: String = Utility.toString(other.getKey)
      other.nextKey
      s
    }
  }

  def ordering: Ordering[String] = KeySet.ordering

  def rangeImpl(from: Option[String], until: Option[String]): KeySet =
    new KeySet(iterGen, from, until)

  override def seq: Set[String] = this.asInstanceOf[Set[String]]
}
