// BSD License (http://lemurproject.org/galago-license)
package julien
package cli

// Aliasing for shorthand
import org.lemurproject.galago.{core => gcore, tupleflow => gt}

import java.io.{File, PrintStream}
import gcore.index.corpus._
import gcore.index.disk.{DiskNameReader,PositionFieldIndexWriter}
import gcore.index.disk.{PositionIndexWriter,PositionContentWriter}
import gcore.util.BuildStageTemplates._
import gcore.parse._
import gcore.types._
import gt.{Order,Utility}
import gt.execution.ConnectionAssignmentType
import gt.execution.{Job,InputStep,MultiStep,OutputStep, Stage, Step}

import scala.collection.mutable.{ListBuffer,HashSet}
import scala.collection.JavaConversions._

/**
  * Refactored from Galago's BuildIndex
  *
  * @author irmarc
  */
object BuildIndex extends TupleFlowFunction {
  val slash = File.separator

  def getOffsetSplitStage: Stage = {
    return new Stage("offsetting").
      addInput("countedSplits", new DocumentSplit.FileNameOrder).
      addOutput("offsetSplits", new DocumentSplit.FileNameOrder).
      add(new InputStep("countedSplits")).
      add(new Step(classOf[SplitOffsetter])).
      add(new OutputStep("offsetSplits"))
  }

  def getCountDocumentsStage: Stage = {
    return new Stage("countDocuments").
      addInput("splits", new DocumentSplit.FileNameOrder()).
      addOutput("countedSplits", new DocumentSplit.FileNameOrder()).
      add(new InputStep("splits")).
      add(new Step(classOf[ParserCounter])).
      add(new OutputStep("countedSplits"))
  }

  def getParsePostingsStage(bp : Parameters) : Stage = {
    val stage = new Stage("parsePostings").
      addInput("offsetSplits", new DocumentSplit.FileNameOrder()).
      addOutput("fieldLengthData", new FieldLengthData.FieldDocumentOrder()).
      addOutput("numberedDocumentDataNumbers",
        new NumberedDocumentData.NumberOrder()).
      addOutput("numberedDocumentDataNames",
        new NumberedDocumentData.IdentifierOrder()).
      addOutput("numberedPostings",
        new NumberWordPosition.WordDocumentPositionOrder()).
      addOutput("numberedExtentPostings",
        new FieldNumberWordPosition.FieldWordDocumentPositionOrder()).
      addOutput("encoded-fields",
        new NumberedField.FieldNameNumberOrder()).
      addOutput("storeKeys",
        new KeyValuePair.KeyOrder())

    // Steps
    stage.add(new InputStep("offsetSplits")).
      add(getParserStep(bp)).
      add(getTokenizerStep(bp))
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
	  new NumberWordPosition.WordDocumentPositionOrder())).
      addGroup("fieldIndex",
	getExtractionSteps("numberedExtentPostings",
	  classOf[NumberedExtentPostingsExtractor],
	  new FieldNumberWordPosition.FieldWordDocumentPositionOrder()))
    // one fork path for each field recorded
    processingFork.addGroup("encode-fields",
      getExtractionSteps("encoded-fields",
        classOf[ContentEncoder],
        bp.getMap("tokenizer"),
        new NumberedField.FieldNameNumberOrder()))

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

    // Make a separate map for corpus stuff
    val storeP = new Parameters
    storeP.set("blockSize", globalParameters.get("corpusBlockSize", 512));
    storeP.set("filename",
      s"${globalParameters.getString("indexPath")}${slash}corpus");
    globalParameters.set("storeParams", storeP);

    // tokenizer/fields must be a list of strings [optional parameter]
    // defaults
    var fieldNames : Set[String] = Set.empty
    if (globalParameters.containsKey("tokenizer")) {
      if (!globalParameters.isMap("tokenizer")) {
        errorLog.add("Parameter 'tokenizer' must be a map.\n")
      } else {
        val tokenizerParams = globalParameters.getMap("tokenizer")
        fieldNames = tokenizerParams.
          getAsList("fields").
          asInstanceOf[java.util.List[String]].
          toSet
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

  def getIndexJob(bp: Parameters) : Job = {
    val indexPath = new File(bp.getString("indexPath")).getAbsolutePath()
    // ensure the index folder exists
    val buildManifest = new File(indexPath, "buildManifest.json")
    Utility.makeParentDirectories(buildManifest)
    Utility.copyStringToFile(bp.toPrettyString(), buildManifest)

    // common steps + connections
    val splitParameters = bp.get("parser", new Parameters()).clone()
    if (bp.isMap("parser")) {
      splitParameters.set("parser", bp.getMap("parser"))
    }

    if (bp.isString("filetype")) {
      splitParameters.set("filetype", bp.getString("filetype"))
    }
    val job = new Job()
    job.add(
      getSplitStage(
        bp.getAsList("inputPath").asInstanceOf[java.util.List[String]],
        classOf[DocumentSource],
        new DocumentSplit.FileNameOrder(),
        splitParameters)).
      add(getCountDocumentsStage).
      each("inputSplit", "countDocuments").
      add(getOffsetSplitStage).
      combined("countDocuments", "offsetting").
      add(getParsePostingsStage(bp)).
      each("offsetting", "parsePostings").
      add(getWritePostingsStage(
        bp,
        "writeContent",
        "encoded-fields",
        new NumberedField.FieldNameNumberOrder,
        "content",
        classOf[PositionContentWriter])).
      combined("parsePostings", "writeContent").
      add(getParallelIndexKeyWriterStage(
        "writeStoreKeys",
        "storeKeys",
        bp.getMap("storeParams"))).
      combined("parsePostings", "writeStoreKeys").
      add(getWriteNamesStage(
        "writeNames",
        new File(indexPath, "names"),
        "numberedDocumentDataNumbers")).
      combined("parsePostings", "writeNames").
      add(getWriteNamesRevStage(
        "writeNamesRev",
        new File(indexPath, "names.reverse"),
        "numberedDocumentDataNames")).
      combined("parsePostings", "writeNamesRev").
      add(getWriteLengthsStage(
        "writeLengths",
        new File(indexPath, "lengths"),
        "fieldLengthData")).
      combined("parsePostings", "writeLengths").
      add(getWritePostingsStage(bp,
        "writePostings",
        "numberedPostings",
        new NumberWordPosition.WordDocumentPositionOrder(),
        "postings",
        classOf[PositionIndexWriter])).
      combined("parsePostings", "writePostings").
      add(getWritePostingsStage(
        bp,
        "writeExtentPostings",
        "numberedExtentPostings",
        new FieldNumberWordPosition.FieldWordDocumentPositionOrder(),
        "field.",
        classOf[PositionFieldIndexWriter])).
      combined("parsePostings", "writeExtentPostings")

    return job
  }

  val name : String = "build"
  def checksOut(p: Parameters): Boolean = (checkBuildIndexParameters(p) == null)
  def help : String = """
  Builds a Galago StructuredIndex with TupleFlow, using one thread
  for each CPU core on your computer.  While some debugging output
  will be displayed on the screen, most of the status information will
  appear on a web page.  A URL should appear in the command output
  that will direct you to the status page.

Required Parameters:
<input>:  Can be either a file or directory, and as many can be
          specified as you like.  Galago can read html, xml, txt,
          arc (Heritrix), warc, trectext, trecweb and store files.
          Files may be gzip compressed (.gz|.bz).
<index>:  The directory path of the index to produce.

Algorithm Flags:
  --tokenizer/fields+{field-name}:
                           Selects field parts to index.
                           default: [none]
"""

  def run(p: Parameters, out: PrintStream) : Unit = {
    val job = getIndexJob(p)
    runTupleFlowJob(job, p, out)
    out.println("Done Indexing.")

    // sanity check - get the number of documents out of ./names
    val names = new DiskNameReader(p.getString("indexPath") + slash + "names")
    out.println("Documents Indexed: " + names.getManifest.getLong("keyCount"))
  }
}
