package julien
package retrieval

import org.scalatest._
import julien.galago.core.index.NullExtentIterator
import julien.access.QuickIndexBuilder

trait FieldIndexBehavior extends QuickIndexBuilder { this: FlatSpec =>

  def aFieldIndex(idx: => Index) {
    it should "complain if a non-existent field is asked for" in {
      intercept[AssertionError] { idx.iterator("baton", "spinderella") }
    }

    it should "provide the vocabulary KeySet over an existing part" in {
      val keys = idx.vocabulary("anchor")
      expect(355) { keys.size }
    }

    it should "provide an extents iterator for a particular field" in {
      val iterator = idx.iterator("chlorociboria", "title")
      assert (iterator != null)
    }

    it should "provide a null extent iterator for an OOV term" in {
      val iterator = idx.iterator("groucho!?!?", "title")
      assert ( iterator.isInstanceOf[NullExtentIterator] )
    }

    it should "probide a lengths iterator for a particular field" in {
      val l = idx.lengthsIterator("title")
      assert (l != null)
    }
  }
}
/** Testing for field support in the Index (when backed by both
  * a DiskIndex and a MemoryIndex).
  */
class FieldsSpec
    extends FlatSpec
    with BeforeAndAfterAll
    with FieldIndexBehavior {

  var didx: Index = null
  var midx: Index = null

  override def beforeAll() {
    didx = makeSampleDisk
    midx = makeSampleMemory
  }

  override def afterAll() {
    deleteSampleDisk
    deleteSampleMemory
  }


  "An index backed by a DiskIndex" should behave like aFieldIndex(didx)

  "An index backed by a MemoryIndex" should
  behave like aFieldIndex(midx)
}
