package julien
package access

import scala.io.Source
import julien.galago.tupleflow.{Parameters,Utility}
import julien.galago.tupleflow.execution.JobExecutor
import julien.galago.core.index.NullExtentIterator
import java.io.{File,InputStream,PrintStream,ByteArrayOutputStream}
import java.util.logging.{Level,Logger}
import org.scalatest._
import julien.cli.BuildIndex

class MemoryIndexSpec
    extends FlatSpec
    with BeforeAndAfterAll
    with QuickIndexBuilder {
  // Our indexes
  var diskIndex: Index = null
  var memoryIndex: Index = null

  // Extract our source docs, write to an index, run the tests over the
  // read-only structure, then clean up.
  override def beforeAll() {
    diskIndex = makeWiki5Disk
    memoryIndex = makeWiki5Memory
  }

  override def afterAll() {
    diskIndex.close
    deleteWiki5Memory
    deleteWiki5Disk
  }

  // Start tests
  "A memory index" should
  "have the same number of docs as the disk version" in {
    expectResult(diskIndex.numDocuments()) { memoryIndex.numDocuments() }
  }

  it should "have the same number of unique terms" in {
    expectResult(diskIndex.vocabularySize()) {
      memoryIndex.vocabularySize()
    }
  }

  it should "have the same collection length" in {
    expectResult(diskIndex.collectionLength()) {
      diskIndex.collectionLength()
    }
  }

  it should "validate a valid lengths iterator for the default key" in {
    val iterator = memoryIndex.lengthsIterator(memoryIndex.defaultField)
    assert ( iterator != null )
  }

  it should "provide the default without arguments as well" in {
    val iterator = memoryIndex.lengthsIterator()
    expectResult(memoryIndex.defaultField) { Utility.toString(iterator.key) }
  }

  it should "provide a null extent iterator for a OOV term" in {
    val iterator = memoryIndex.extents("snufalufagus")
    assert ( iterator.isInstanceOf[NullExtentIterator] )
  }

  it should "provide a null counts iterator for a OOV term" in {
    val iterator = memoryIndex.counts("snufalufagus")
    assert ( iterator.isInstanceOf[NullExtentIterator] )
  }

  it should "fail an assertion when asking for a non-existent part" in {
    intercept[AssertionError] { memoryIndex.extents("chocula", "bert") }
  }
}
