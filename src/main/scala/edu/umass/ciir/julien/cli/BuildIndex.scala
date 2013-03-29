// BSD License (http://lemurproject.org/galago-license)
package edu.umass.ciir.julien.cli

import java.io.File
import java.io.PrintStream
import org.lemurproject.galago.core.index.corpus._
import org.lemurproject.galago.core.index.disk.DiskNameReader
import org.lemurproject.galago.core.index.disk.PositionFieldIndexWriter
import org.lemurproject.galago.core.index.disk.PositionIndexWriter
import org.lemurproject.galago.core.util.BuildStageTemplates._
import org.lemurproject.galago.core.parse._
import org.lemurproject.galago.core.types._
import org.lemurproject.galago.tupleflow.Order
import org.lemurproject.galago.tupleflow.Parameters
import org.lemurproject.galago.tupleflow.Utility
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType
import org.lemurproject.galago.tupleflow.execution.InputStep
import org.lemurproject.galago.tupleflow.execution.Job
import org.lemurproject.galago.tupleflow.execution.MultiStep
import org.lemurproject.galago.tupleflow.execution.OutputStep
import org.lemurproject.galago.tupleflow.execution.Stage
import org.lemurproject.galago.tupleflow.execution.Step
import edu.umass.ciir.julien.Utils._

import scala.collection.mutable.{ListBuffer,HashSet}
import scala.collection.JavaConversions._

/**
  * Refactored from Galago's BuildIndex
  *
  * @author irmarc
  */
object BuildIndex extends TupleFlowFunction {
  val slash = File.separator

  def getParsePostingsStage(bp : Parameters) : Stage = {
    val stage = new Stage("parsePostings").
      addInput("splits", new DocumentSplit.FileIdOrder()).
      addOutput("fieldLengthData", new FieldLengthData.FieldDocumentOrder()).
      addOutput("numberedDocumentDataNumbers",
        new NumberedDocumentData.NumberOrder()).
      addOutput("numberedDocumentDataNames",
        new NumberedDocumentData.IdentifierOrder()).
      addOutput("storeKeys", new KeyValuePair.KeyOrder()).
      addOutput("numberedPostings",
        new NumberWordPosition.WordDocumentPositionOrder())

    if (!bp.getMap("tokenizer").getList("fields").isEmpty()) {
      stage.addOutput("numberedExtents",
        new NumberedExtent.ExtentNameNumberBeginOrder())
    }
    if (!bp.getMap("tokenizer").getMap("formats").isEmpty()) {
      stage.addOutput("numberedFields",
        new NumberedField.FieldNameNumberOrder())
    }

    if (bp.getBoolean("fieldIndex")) {
      stage.addOutput("numberedExtentPostings",
        new FieldNumberWordPosition.FieldWordDocumentPositionOrder())
    }

    // Steps
    stage.add(new InputStep("splits")).
      add(getParserStep(bp)).
      add(getTokenizerStep(bp)).
      add(getNumberingStep(bp))
    val processingFork = new MultiStep()

    // these forks are always executed
    processingFork.
      addGroup("fieldLengths",
	getExtractionSteps("fieldLengthData",
	  classOf[FieldLengthExtractor],
	  new FieldLengthData.FieldDocumentOrder())).
      addGroup("numberedDocumentData",
	getExtractionSteps("numberedDocumentDataNumbers",
	  classOf[NumberedDocumentDataExtractor],
	  new NumberedDocumentData.NumberOrder())).
      addGroup("numberedDocumentDataNames",
	getExtractionSteps("numberedDocumentDataNames",
	  classOf[NumberedDocumentDataExtractor],
	  new NumberedDocumentData.IdentifierOrder())).
      addGroup("store").
      addToGroup("store",
        new Step(classOf[CorpusFolderWriter], bp.getMap("storeParams"))).
      addToGroup("store", Utility.getSorter(new KeyValuePair.KeyOrder())).
      addToGroup("store", new OutputStep("storeKeys")).
      addGroup("postings",
        getExtractionSteps("numberedPostings",
	  classOf[NumberedPostingsPositionExtractor],
	  new NumberWordPosition.WordDocumentPositionOrder()))

    // now optional forks
    if (!bp.getMap("tokenizer").getList("fields").isEmpty()) {
      processingFork.addGroup("extents",
	getExtractionSteps("numberedExtents",
	  classOf[NumberedExtentExtractor],
	  new NumberedExtent.ExtentNameNumberBeginOrder()))
    }
    if (!bp.getMap("tokenizer").getMap("formats").isEmpty()) {
      processingFork.addGroup("comparable Fields",
	getExtractionSteps("numberedFields",
	  classOf[NumberedFieldExtractor],
	  bp,
	  new NumberedField.FieldNameNumberOrder()))
    }

    if (bp.getBoolean("fieldIndex")) {
      processingFork.addGroup("fieldIndex",
	getExtractionSteps("numberedExtentPostings",
	  classOf[NumberedExtentPostingsExtractor],
	  new FieldNumberWordPosition.FieldWordDocumentPositionOrder()))
    }
    return stage.add(processingFork)
  }

  def getWritePostingsStage(
    bp: Parameters,
    stageName: String,
    inputName: String,
    inputOrder: Order[_],
    indexName: String,
    indexWriter: Class[_]) : Stage = {
    val p = new Parameters()
    p.set("filename", bp.getString("indexPath") + slash + indexName)
    p.set("skipping", bp.getBoolean("skipping"))
    p.set("skipDistance", bp.getLong("skipDistance"))
    return new Stage(stageName).addInput(inputName, inputOrder).
      add(new InputStep(inputName)).
      add(new Step(indexWriter, p))
  }

  def getParallelIndexKeyWriterStage(
    name: String,
    input: String,
    indexParameters: Parameters) : Stage = {
    return new Stage(name).addInput(input, new KeyValuePair.KeyOrder()).
      add(new InputStep(input)).
      add(new Step(classOf[SplitBTreeKeyWriter], indexParameters))
  }

  def checkBuildIndexParameters(globalParameters: Parameters) : Parameters = {
    val errorLog = ListBuffer[String]()

    // inputPath may be a string, or a list of strings -- required
    if (!globalParameters.containsKey("inputPath")) {
      errorLog.add("Parameter 'inputPath' is required " +
        "as a string or list of strings.")
    } else {
      val absolutePaths = globalParameters.getAsList("inputPath").map { s =>
        new File(s.asInstanceOf[String]).getAbsolutePath
      }
      globalParameters.remove("inputPath")
      globalParameters.set("inputPath", absolutePaths)
    }

    // indexPath may be a string
    if (!globalParameters.containsKey("indexPath")) {
      errorLog.add("Parameter 'indexPath' is required. It should be a string.")
    } else {
    val indexPath = globalParameters.getString("indexPath")
    globalParameters.set("indexPath", (new File(indexPath).getAbsolutePath()))
    }

    // ensure there are some default parameters
    val storeParams = new Parameters()
    storeParams.set("readerClass", classOf[CorpusReader].getName())
    storeParams.set("writerClass", classOf[CorpusFolderWriter].getName())
    // we need a small block size because the stored values are small
    storeParams.set("blockSize", globalParameters.get("storeBlockSize", 512))
    storeParams.set("filename",
      globalParameters.getString("indexPath") + slash + "store")

    // copy from the other parameters in case the client added some manually
    if (globalParameters.isMap("storeParams")) {
      storeParams.copyFrom(globalParameters.getMap("storeParams"))
    }

    // insert back into the globalParams
    globalParameters.set("storeParams", storeParams)

    // tokenizer/fields must be a list of strings [optional parameter]
    // defaults
    var fieldNames : Set[String] = Set.empty
    if (globalParameters.containsKey("tokenizer")) {
      if (!globalParameters.isMap("tokenizer")) {
        errorLog.add("Parameter 'tokenizer' must be a map.\n")
      } else {
        val tokenizerParams = globalParameters.getMap("tokenizer")
        fieldNames =
          tokenizerParams.getAsList("fields").asInstanceOf[List[String]].toSet
        if (!fieldNames.isEmpty) globalParameters.set("fieldIndex", true)

        // tokenizer/format is a mapping from fields to types [optional]
        //  each type needs to be indexable {string,int,long,float,double,date}
        if (tokenizerParams.isMap("formats")) {
          val formats = tokenizerParams.getMap("formats")
          val pat = """string|int|long|float|double|date""".r
          if (formats.getKeys.exists(!fieldNames(_))) {
            errorLog.add("Found a format for one or more unknown fields: " +
              formats.getKeys.filter(fieldNames(_)).mkString(","))
          }
          val fieldTypes = formats.getKeys.map(formats.getString(_))
          if (fieldTypes.exists(ft => pat misses ft)) {
            errorLog.add("Unknown format(s): " +
              fieldTypes.filter(ft => pat misses ft).mkString(","))
          }
        }
      }
    }

    if (errorLog.isEmpty()) {
      return globalParameters
    } else {
      for (err <- errorLog) {
        Console.err.println(err)
      }
      return null
    }
  }

  def getIndexJob(unchecked: Parameters) : Job = {
    val bp = checkBuildIndexParameters(unchecked)
    if (bp == null) {
      return null
    }

    val indexPath = new File(bp.getString("indexPath")).getAbsolutePath()
    // ensure the index folder exists
    val buildManifest = new File(indexPath, "buildManifest.json")
    Utility.makeParentDirectories(buildManifest)
    Utility.copyStringToFile(bp.toPrettyString(), buildManifest)

    // common steps + connections
    val splitParameters = bp.get("parser", new Parameters()).clone()
    splitParameters.set("storePieces", bp.get("distrib", 10))
    if (bp.isMap("parser")) {
      splitParameters.set("parser", bp.getMap("parser"))
    }

    if (bp.isString("filetype")) {
      splitParameters.set("filetype", bp.getString("filetype"))
    }
    val job = new Job()
    job.add(getSplitStage(
      bp.getAsList("inputPath").asInstanceOf[List[String]],
      classOf[DocumentSource],
      new DocumentSplit.FileIdOrder(),
      splitParameters)).
      add(getParsePostingsStage(bp)).
      add(getWriteNamesStage(
        "writeNames",
        new File(indexPath, "names"),
        "numberedDocumentDataNumbers")).
      add(getWriteNamesRevStage(
        "writeNamesRev",
        new File(indexPath, "names.reverse"),
        "numberedDocumentDataNames")).
      add(getWriteLengthsStage(
        "writeLengths",
        new File(indexPath, "lengths"),
        "fieldLengthData")).
      each("inputSplit", "parsePostings").
      combined("parsePostings", "writeLengths").
      combined("parsePostings", "writeNames").
      combined("parsePostings", "writeNamesRev").
      add(getParallelIndexKeyWriterStage(
        "writeStoreKeys",
        "storeKeys",
        bp.getMap("storeParams"))).
      combined("parsePostings", "writeStoreKeys").
      add(getWritePostingsStage(bp,
        "writePostings",
        "numberedPostings",
        new NumberWordPosition.WordDocumentPositionOrder(),
        "postings",
        classOf[PositionIndexWriter])).
      combined("parsePostings", "writePostings")

    // if we have at least one field - write extents
    if (!bp.getMap("tokenizer").getList("fields").isEmpty()) {
      job.add(getWriteExtentsStage(
        "writeExtents",
        new File(indexPath, "extents"),
        "numberedExtents")).
        combined("parsePostings", "writeExtents")
    }

    // if we have at least one field format - write fields
    if (!bp.getMap("tokenizer").getMap("formats").isEmpty()) {
      val p = new Parameters()
      p.set("tokenizer", bp.getMap("tokenizer"))
      job.add(getWriteFieldsStage(
        "writeFields",
        new File(indexPath, "fields"),
        "numberedFields",
        p)).
        combined("parsePostings", "writeFields")
    }

    // field indexes
    if (bp.getBoolean("fieldIndex")) {
      job.add(getWritePostingsStage(
        bp,
        "writeExtentPostings",
        "numberedExtentPostings",
        new FieldNumberWordPosition.FieldWordDocumentPositionOrder(),
        "field.",
        classOf[PositionFieldIndexWriter])).
        combined("parsePostings", "writeExtentPostings")
    }
    return job
  }

  def name : String = "build"

  def help : String =
    """galago build [flags] --indexPath=<index> (--inputPath+<input>)

  Builds a Galago StructuredIndex with TupleFlow, using one thread
  for each CPU core on your computer.  While some debugging output
  will be displayed on the screen, most of the status information will
  appear on a web page.  A URL should appear in the command output
  that will direct you to the status page.

<input>:  Can be either a file or directory, and as many can be
          specified as you like.  Galago can read html, xml, txt,
          arc (Heritrix), warc, trectext, trecweb and store files.
          Files may be gzip compressed (.gz|.bz).
<index>:  The directory path of the index to produce.

Algorithm Flags:
  --tokenizer/fields+{field-name}:
                           Selects field parts to index.
                           [omitted]
"""

  def run(p: Parameters, out: PrintStream) : Unit = {
    // build index input
    if (!p.isString("indexPath") && !p.isList("inputPath")) {
      out.println(help)
      return
    }

    val job = getIndexJob(p)
    runTupleFlowJob(job, p, out)
    out.println("Done Indexing.")

    // sanity check - get the number of documents out of ./names
    val names = new DiskNameReader(p.getString("indexPath") + slash + "names")
    out.println("Documents Indexed: " + names.getManifest.getLong("keyCount"))
  }
}
