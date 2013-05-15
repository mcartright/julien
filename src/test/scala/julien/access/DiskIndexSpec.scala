package julien
package access

import scala.io.Source
import julien.galago.core.index.NullExtentIterator
import org.scalatest._
import julien.galago.tupleflow.Utility

class DiskIndexSpec
    extends FlatSpec
    with BeforeAndAfterAll
    with QuickIndexBuilder {

  var index: Index = null

  // Extract our source docs, write to an index, run the tests over the
  // read-only structure, then clean up.
  override def beforeAll() { index = makeWiki5Disk }

  override def afterAll() {
    index.close
    deleteWiki5Disk
  }

  // Start tests
  "A built index" should "have 5 documents" in {
    expectResult(5L) { index.numDocuments } // currently we have some loss
  }

  it should "have 86 unique terms" in {
    expectResult(86L) { index.vocabularySize }
  }

  it should "have 123 term instances" in {
    expectResult(123L) { index.collectionLength }
  }

  it should "provide a valid lengths iterator for the default field" in {
    val iterator = index.lengthsIterator(index.defaultField)
    assert( iterator != null )
  }

  it should "provide the default without arguments as well" in {
    val iterator = index.lengthsIterator()
    expectResult(index.defaultField) { Utility.toString(iterator.key) }
  }

  it should "provide a null extent iterator for a OOV term" in {
    val iterator = index.iterator("snufalufagus")
    assert ( iterator.isInstanceOf[NullExtentIterator] )
  }

  it should "fail an assertion when asking for a non-existent part" in {
    intercept[AssertionError] { index.iterator("chocula", "bert") }
  }
}
