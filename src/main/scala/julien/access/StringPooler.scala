package julien
package access

import scala.collection.mutable.{ListBuffer, WeakHashMap}
import scala.ref.WeakReference

object StringPooler {
  val defaultCacheSize = 100000
  def apply() = new StringPooler(defaultCacheSize)
  def apply(s: Long) = new StringPooler(s)
}

class StringPooler private(maxSize: Long) {
  private val pool = WeakHashMap[String, WeakReference[String]]()

  def collapse(terms: Array[String]): Array[String] = {
    if (maxSize > 0 && pool.size > maxSize) pool.clear
    val builder = List.newBuilder[String]
    var i = 0
    while (i < terms.length) {
      val t = terms(i)
      val ref = pool.get(t) // get the WeakRef
      val insert = if (ref.isDefined) {
        ref.get.get.asInstanceOf[String]
      } else {
        pool.put(t, WeakReference(t))
        t
      }
      terms(i) = insert
    }
    terms
  }
}
