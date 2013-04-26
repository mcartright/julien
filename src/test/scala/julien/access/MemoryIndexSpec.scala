package julien
package access

import scala.io.Source
import org.lemurproject.galago.tupleflow.{Parameters,Utility}
import org.lemurproject.galago.tupleflow.execution.JobExecutor
import org.lemurproject.galago.core.index.NullExtentIterator
import java.io.{File,InputStream,PrintStream,ByteArrayOutputStream}
import java.util.logging.{Level,Logger}
import org.scalatest._
import julien.cli.BuildIndex

class MemoryIndexSpec extends FlatSpec with BeforeAndAfterAll {
  val tmpForInput: File =
    new File(Utility.createTemporary.getAbsolutePath)
  val tmpForIndex: File = Utility.createTemporaryDirectory("tmpindex")
  // Our indexes
  var diskIndex: Index = null
  var memoryIndex: Index = null

  // Extract our source docs, write to an index, run the tests over the
  // read-only structure, then clean up.
  override def beforeAll() {
    // Turn off logging so the tests aren't diluted.
    Logger.getLogger("").setLevel(Level.OFF)
    Logger.getLogger(classOf[JobExecutor].toString).setLevel(Level.OFF)

    // Extract resource - small 5 doc collection for correctness testing
    val istream = getClass.getResourceAsStream("/wiki-trectext-5.dat")
    assert (istream != null)
    Utility.copyStreamToFile(istream, tmpForInput)

    // Set some parameters
    val params = new Parameters
    params.set("inputPath", tmpForInput.getAbsolutePath)
    params.set("indexPath", tmpForIndex.getAbsolutePath)
    params.set("deleteJobDir", true)
    params.set("mode", "local")
    val parser = new Parameters
    params.set("parser", parser)
    params.set("corpus", true)
    val corpus = new Parameters
    val corpusFile =
      new File(tmpForIndex.getAbsolutePath, "corpus")
    corpusFile.mkdir
    corpus.set("filename", corpusFile.getAbsolutePath)
    params.set("corpusParameters", corpus)

    // Sort of a "/dev/null"
    val receiver = new PrintStream(new ByteArrayOutputStream)
    BuildIndex.run(params, receiver)
    receiver.close
    diskIndex = Index.disk(tmpForIndex.getAbsolutePath)

    // Make the memory index too
    memoryIndex = Index.memory(tmpForInput.getAbsolutePath)
  }

  override def afterAll() {

    diskIndex.close
    tmpForInput.delete()
    Utility.deleteDirectory(tmpForIndex)

    memoryIndex.close
  }

  // Start tests
  "A memory index" should
  "have the same number of docs as the disk version" in {
    expectResult(diskIndex.numDocuments) { memoryIndex.numDocuments }
  }

  it should "have the same number of unique terms" in {
    expectResult(diskIndex.vocabularySize) {
      memoryIndex.vocabularySize
    }
  }

  it should "have the same collection length" in {
    expectResult(diskIndex.collectionLength) {
      diskIndex.collectionLength
    }
  }

  it should "validate a valid lengths iterator for the default key" in {
    val iterator = memoryIndex.lengthsIterator(memoryIndex.defaultPart)
    assert ( iterator != null )
  }

  it should "provide the default without arguments as well" in {
    val iterator = memoryIndex.lengthsIterator()
    expect(memoryIndex.defaultPart) { Utility.toString(iterator.key) }
  }

  it should "provide a null extent iterator for a OOV term" in {
    val iterator = memoryIndex.iterator("snufalufagus")
    assert ( iterator.isInstanceOf[NullExtentIterator] )
  }

  it should "fail an assertion when asking for a non-existent part" in {
    intercept[AssertionError] { memoryIndex.iterator("chocula", "bert") }
  }
}
