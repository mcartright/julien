package julien
package access

import julien.galago.tupleflow.{Parameters,Utility}
import julien.galago.tupleflow.execution.JobExecutor
import java.io.{File,InputStream,PrintStream,ByteArrayOutputStream}
import java.util.logging.{Level,Logger}
import julien.cli.BuildIndex
import scala.collection.JavaConversions._

trait QuickIndexBuilder {
  val tmp5Input: File = Utility.createTemporary
  val tmp5Index: File = Utility.createTemporaryDirectory("tmpindex")

  def copy5: Unit = {
    val istream = getClass.getResourceAsStream("/wiki-trectext-5.dat")
    assert (istream != null)
    debug(s"Copying to ${tmp5Input.getAbsolutePath}")
    Utility.copyStreamToFile(istream, tmp5Input)
  }

  def makeWiki5Disk: Index = {
    // Turn off logging so the tests aren't diluted.
    Logger.getLogger("").setLevel(Level.OFF)
    Logger.getLogger(classOf[JobExecutor].toString).setLevel(Level.OFF)

    // Extract resource - small 5 doc collection for checking statistics
    copy5

    // Set some parameters
    val params = new Parameters
    params.set("inputPath", tmp5Input.getAbsolutePath)
    params.set("indexPath", tmp5Index.getAbsolutePath)
    params.set("deleteJobDir", true)
    params.set("mode", "local")
    val parser = new Parameters
    params.set("parser", parser)
    params.set("corpus", true)
    val corpus = new Parameters
    val corpusFile =
      new File(tmp5Index.getAbsolutePath, "corpus")
    corpusFile.mkdir
    corpus.set("filename", corpusFile.getAbsolutePath)
    params.set("corpusParameters", corpus)

    // Sort of a "/dev/null"
    val receiver = new PrintStream(new ByteArrayOutputStream)

    BuildIndex.run(params, receiver)
    receiver.close
    Index.disk(tmp5Index.getAbsolutePath)
  }

  def deleteWiki5Disk: Unit = {
    if (tmp5Input.exists) tmp5Input.delete()
    Utility.deleteDirectory(tmp5Index)
  }

  def deleteWiki5Memory: Unit = {
    if (tmp5Input.exists) tmp5Input.delete()
  }

  def makeWiki5Memory: Index = {
    copy5
    Index.memory(tmp5Input.getAbsolutePath)
  }

  val tmpSampInput: File =
    new File(Utility.createTemporary.getAbsolutePath + ".gz")
  val tmpSampIndex: File = Utility.createTemporaryDirectory("tmpindex")

  def deleteSampleDisk: Unit = {
    if (tmpSampInput.exists) tmpSampInput.delete()
    Utility.deleteDirectory(tmpSampIndex)
  }

  def deleteSampleMemory: Unit = {
    if (tmpSampInput.exists) tmpSampInput.delete()
  }

  def copySample: Unit = {
    // Extract resource - small 5 doc collection for correctness testing
    val istream = getClass.getResourceAsStream("/wikisample.gz")
    assert (istream != null)
    Utility.copyStreamToFile(istream, tmpSampInput)
  }

  def makeSampleDisk: Index = {
    Logger.getLogger("").setLevel(Level.OFF)
    Logger.getLogger(classOf[JobExecutor].toString).setLevel(Level.OFF)
    copySample

    // Set some parameters
    val params = new Parameters
    params.set("inputPath", tmpSampInput.getAbsolutePath)
    params.set("indexPath", tmpSampIndex.getAbsolutePath)
    params.set("deleteJobDir", true)
    params.set("mode", "local")
    val parser = new Parameters
    parser.set("filetype", "wikiwex")
    params.set("parser", parser)
    val tokenizer = new Parameters
    tokenizer.set("fields",
      List("title", "title-exact", "fbname",
        "fbname-exact", "fbtype", "category",
        "redirect", "redirect-exact", "kb_class",
        "anchor", "anchor-exact", "text",
        "stanf_anchor", "stanf_anchor-exact"))
    params.set("tokenizer", tokenizer)
    params.set("corpus", true)
    val corpus = new Parameters
    val corpusFile =
      new File(tmpSampIndex.getAbsolutePath, "corpus")
    corpusFile.mkdir
    corpus.set("filename", corpusFile.getAbsolutePath)
    params.set("corpusParameters", corpus)
    val receiver = new PrintStream(new ByteArrayOutputStream)
    BuildIndex.run(params, receiver)
    receiver.close
    Index.disk(tmpSampIndex.getAbsolutePath)
  }

  def makeSampleMemory: Index = {
    copySample
    val parserParams = new Parameters()
    parserParams.set("filetype", "wikiwex")
    val tokenizerParams = new Parameters()
    tokenizerParams.set("fields",
      List("title", "title-exact", "fbname",
        "fbname-exact", "fbtype", "category",
        "redirect", "redirect-exact", "kb_class",
        "anchor", "anchor-exact", "text",
        "stanf_anchor", "stanf_anchor-exact"))
    val indexParams = new Parameters()
    indexParams.set("parser", parserParams)
    indexParams.set("tokenizer", tokenizerParams)
    indexParams.set("filetype", "wikiwex")
    Index.memory(tmpSampInput.getAbsolutePath, "all", indexParams)
  }
}
