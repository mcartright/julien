// BSD License (http://lemurproject.org/galago-license)
package julien
package cli

// Aliasing for shorthand
import julien.galago.{core => gcore, tupleflow => gt}

import java.io.{File, PrintStream}
import gcore.index.corpus._
import gcore.index.disk._
import gcore.parse._
import gcore.types._

import julien.galago.tupleflow.Utility

import collection.mutable.{ListBuffer,HashSet}
import collection.JavaConversions._

import julien.flow._
import Recipe._


/**
  * Refactored from Galago's BuildIndex
  *
  * @author irmarc
  */
object BuildIndex extends TupleFlowFunction {
  val slash = File.separator

  // check parameters
  def checkBuildIndexParameters(gblParms: Parameters) : Parameters = {
    val errorLog = new StringBuilder

    // inputPath may be a string, or a list of strings -- required
    if (!gblParms.containsKey("inputPath")) {
      errorLog ++= "Parameter 'inputPath' is required as a string or list of strings.\n"
    } else {
    }

    // indexPath may be a string
    if (!gblParms.containsKey("indexPath")) {
      errorLog ++= "Parameter 'indexPath' is required. It should be a string.\n"
    } else {
      val indexPath = gblParms.getString("indexPath")
      gblParms.set("indexPath", (new File(indexPath).getCanonicalPath()))
    }

    // Make a separate map for corpus stuff
    val storeP = new Parameters
    storeP.set("blockSize", gblParms.get("corpusBlockSize", 512));
    storeP.set("filename",
      s"${gblParms.getString("indexPath")}${slash}corpus");
    gblParms.set("storeParams", storeP);

    // tokenizer/fields must be a list of strings [optional parameter]
    // defaults
    var fieldNames : Set[String] = Set.empty
    val tokenizerParams = new Parameters()
    if (gblParms.containsKey("tokenizer")) {
      if (!gblParms.isMap("tokenizer")) {
        errorLog ++= "Parameter 'tokenizer' must be a map.\n"
      } else {
        val tokenizerParams = gblParms.getMap("tokenizer")
        fieldNames = tokenizerParams.
          getAsList("fields").
          asInstanceOf[java.util.List[String]].
          toSet
      }
    } else {
      gblParms.set("tokenizer", tokenizerParams)
    }
    tokenizerParams.set("fields", fieldNames)
    tokenizerParams.set("formats", new Parameters())

    if(gblParms.containsKey("parser") && !gblParms.isMap("parser")) {
      errorLog ++= "parser ought to be a map!"
      gblParms.remove("parser")
    }
    // handle not-present
    gblParms.set("parser", gblParms.get("parser", new Parameters))

    if (errorLog.isEmpty()) {
      return gblParms
    } else {
      Console.err.println(errorLog)
      throw new RuntimeException("Unable to execute job: " +errorLog)
    }
  }

  def getSplitParms(bp: Parameters): Parameters = {
    val splitParms = bp.getMap("parser").clone()
    if(bp.isString("filetype")) {
      splitParms.set("filetype", bp.getString("filetype"))
    }
    
    // filter input paths into files and directories
    val inputPaths = bp.getAsList("inputPath").asInstanceOf[java.util.List[String]].toSet
    var files = new ListBuffer[String]
    var directories = new ListBuffer[String]

    inputPaths.foreach(path => {
      val fp = new File(path)
      if(fp.isFile) { files += fp.getCanonicalPath }
      else if(fp.isDirectory) { directories += fp.getCanonicalPath }
      else {
        Console.err.println("Cannot find input path: "+path)
        ???
      }
    })
    splitParms.set("filename", files)
    splitParms.set("directory", directories)
    
    splitParms
  }

  def constructJob(bp: Parameters): Job = {
    writeBuildManifest(bp)

    // build up stages
    val splitInput = new FlowStage(FlowLinearStep(Seq(
        FlowStep(classOf[DocumentSource], getSplitParms(bp)),
        FlowStep(new DocumentSplit.FileNameOrder())
    )), gensym("DocumentSource"))

    val countDocs = FlowStage(classOf[ParserCounter])
    val offsetDocs = FlowStage(classOf[SplitOffsetter])
    
    // build up output of fork stage first
    val writeNames = FlowStage(classOf[DiskNameWriter], indexFileParms(bp, "names"))
    val writeNamesRev = FlowStage(classOf[DiskNameReverseWriter], indexFileParms(bp, "names.reverse"))
    val writeLengths = FlowStage(classOf[DiskLengthsWriter], indexFileParms(bp, "lengths"))
    // PositionFieldIndexWriter uses this name as a prefix to calculate names
    val writeExtentPostings = FlowStage(classOf[PositionFieldIndexWriter], indexFileParms(bp, ""))

    val writeContent = if(bp.get("content", true)) {
      Some(FlowStage(classOf[PositionContentWriter],indexFileParms(bp, "content")))
    } else None

    val writeCorpus = if(bp.get("corpus", true)) {
      Some(FlowStage(classOf[SplitBTreeKeyWriter], bp.getMap("storeParams")))
    } else None

    val writePostings = if(bp.get("nonStemmedPostings", true)) {
      Some(FlowStage(classOf[PositionIndexWriter], indexFileParms(bp, "all.postings")))
    } else None

    // build up complicated fork stage
    val parseDocs = {
      val leadup = Seq(
        FlowStep(classOf[ParserSelector], bp.getMap("parser")),
        FlowStep(classOf[TagTokenizer], bp.getMap("tokenizer"))
      )

      val branches = Seq(
        extractor(classOf[FieldLengthExtractor], Some(writeLengths)),
        extractor(classOf[NumberedDocumentDataExtractor], Some(writeNames)),
        extractor(classOf[NumberedDocumentDataExtractor], Some(writeNamesRev)),
        extractor(classOf[NumberedExtentPostingsExtractor], Some(writeExtentPostings)),

        //--- optional bits:
        extractor(classOf[NumberedPostingsPositionExtractor], writePostings),
        extractor(classOf[ContentEncoder], writeContent, bp.getMap("tokenizer")),

        // enforce custom sort on CorpusFolderWriter;
        // it doesn't need a sort but is faster with one
        extractor(classOf[CorpusFolderWriter], writeCorpus, bp.getMap("storeParams"), Some(new KeyValuePair.KeyOrder()))

      ).flatten // remove anything that wasn't included in the build
      
      new FlowStage(FlowLinearStep(leadup ++ Seq(FlowMultiStep(branches))), gensym("ParseDocs"))
    }

    // DSL for when we don't want to create nodes explicitly
    // splitTo = each
    // joinTo = combined
    splitInput splitTo countDocs
    countDocs joinTo offsetDocs
    offsetDocs splitTo parseDocs

    // the rest of the stages will be joined automagically by sharing nodes
    // build up our set of stages
    val inputStages: Set[FlowStage] = Set(splitInput, countDocs, offsetDocs, parseDocs)
    val mustWriteStages: Set[FlowStage] = Set(writeNames, writeNamesRev, writeLengths, writeExtentPostings)
    val mightWriteStages: Set[FlowStage] = Set(writeCorpus, writeContent, writePostings).flatten

    val graph = inputStages ++ 
                mustWriteStages ++
                mightWriteStages
    
    JobGen.createAndVerify(graph)
  }

  val name : String = "build"
  def checksOut(p: Parameters): Boolean =
    try {
      (checkBuildIndexParameters(p) != null)
    } catch {
      case e: Exception =>
        false
    }
  
  def help : String = """
  Builds a Galago StructuredIndex with TupleFlow, using one thread
  for each CPU core on your computer.  While some debugging output
  will be displayed on the screen, most of the status information will
  appear on a web page.  A URL should appear in the command output
  that will direct you to the status page.

Required Parameters:
<inputPath>:  Can be either a file or directory, and as many can be
          specified as you like.  Galago can read html, xml, txt,
          arc (Heritrix), warc, trectext, trecweb and store files.
          Files may be gzip compressed (.gz|.bz).
<indexPath>:  The directory path of the index to produce.

Algorithm Flags:
  --tokenizer/fields+{field-name}:
                           Selects field parts to index.
                           default: [none]
"""

  def run(p: Parameters, out: PrintStream) : Unit = {
    val bp = checkBuildIndexParameters(p)

    val job = constructJob(bp)
    runTupleFlowJob(job, bp, out)
    out.println("Done Indexing!")
    
    // sanity check - get the number of documents out of ./names
    val names = new DiskNameReader(new File(bp.getString("indexPath"), "names").getCanonicalPath)
    out.println("Documents Indexed: " + names.getManifest.getLong("keyCount"))

    // sometimes local threaded hangs, so force exit here
    sys.exit(0);
  }
}

