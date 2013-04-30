package julien
package retrieval

import org.scalatest._

trait FieldIndexBehavior { this: FlatSpec =>

  def aFieldIndex(idx: => Index) {
    it should "complain if a non-existent field is asked for" in (pending)
    it should "provide an extents iterator for a particular field" in (pending)
    it should "provide a null extent iterator for an OOV term" in (pending)
    it should "probide a lengths iterator for a particular field" in (pending)
  }
}
/** Testing for field support in the Index (when backed by both
  * a DiskIndex and a MemoryIndex).
  */
class FieldsSpec
    extends FlatSpec
    with FieldIndexBehavior {

  /*
  "An index backed by a DiskIndex" should behave like aFieldIndex(didx)

  "An index backed by a MemoryIndex" should
  behave like aFieldIndex(midx)
   */
}
