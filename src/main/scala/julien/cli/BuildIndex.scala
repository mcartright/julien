// BSD License (http://lemurproject.org/galago-license)
package julien
package cli

// Aliasing for shorthand
import julien.galago.{core => gcore, tupleflow => gt}

import java.io.{File, PrintStream}
import gcore.index.corpus._
import gcore.index.disk._
import gcore.parse._
import gcore.parse.stem._
import gcore.types._

import julien.galago.tupleflow.Utility

import collection.mutable.{ListBuffer,HashSet}
import collection.JavaConversions._

import julien.flow._
import Recipe._

/** Refactored from Galago's BuildIndex
  *
  * @author irmarc
  * @author jfoley
  */
object BuildIndex extends TupleFlowFunction {
  // We'll add the option back in later.
  val stemmers = Map(
    classOf[NullStemmer] -> "raw",
    classOf[Porter2Stemmer] -> "porter"
  )
  val slash = File.separator

  // check parameters
  def checkBuildIndexParameters(gblParms: Parameters) : Parameters = {
    val errorLog = new StringBuilder

    // inputPath may be a string, or a list of strings -- required
    if (!gblParms.containsKey("inputPath")) {
      errorLog ++=
      "Parameter 'inputPath' is required as a string or list of strings.\n"
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
    val inputPaths = bp.getAsList("inputPath").toSet.asInstanceOf[Set[String]]
    val inputFiles = inputPaths.map(p => new File(p))
    // check we have valid inputs
    inputFiles.foreach { f =>
      assume(f.exists && (f.isFile || f.isDirectory),
        s"File '${f.getPath}' either doesn't exist or is not file/directory.")
    }
    val (files, directories) = inputFiles.partition(_.isFile)
    splitParms.set("filename", files.map(_.getAbsolutePath))
    splitParms.set("directory", directories.map(_.getAbsolutePath))
    splitParms
  }

  def stemBranches(
    bp: Parameters
  ): Tuple2[Seq[FlowLinearStep], Seq[FlowStage]] = {
    val pathways = Seq.newBuilder[FlowLinearStep]
    val writeStages = Seq.newBuilder[FlowStage]
   // For each stemmer:
    for ((stemmer, suffix) <- stemmers) {
      // 1) Make the main writer
      val postingsWriter =
        FlowStage(classOf[PositionIndexWriter],
          indexFileParms(bp, s"all.postings.${suffix}"))
      writeStages += postingsWriter
      val postingsOrder = postingsWriter.inputSortOrder
      // 2) Need to create a chain of (stemmer -> NPPE -> writer) -> pathways
      val postingsChain = Seq(
        FlowStep(stemmer),
        FlowStep(classOf[NumberedPostingsPositionExtractor]),
        FlowStep(postingsOrder.get),
        FlowOutput(postingsWriter.makeInputNode(FlowNodeJoin(), postingsOrder))
      )
      pathways += FlowLinearStep(postingsChain)

      // 3) Make a field writer
      val p = new Parameters()
      p.set("filename", bp.getString("indexPath") + slash)
      p.set("suffix", s".$suffix")
      val fieldsWriter =
        FlowStage(classOf[PositionFieldIndexWriter], p)
      writeStages += fieldsWriter
      val fieldsOrder = fieldsWriter.inputSortOrder
      // 4) Create a chain of (stemmer -> NEPE -> field writer) -> pathways
      val fieldsChain = Seq(
        FlowStep(stemmer),
        FlowStep(classOf[NumberedExtentPostingsExtractor]),
        FlowStep(fieldsOrder.get),
        FlowOutput(fieldsWriter.makeInputNode(FlowNodeJoin(), fieldsOrder))
      )
      pathways += FlowLinearStep(fieldsChain)
    }
    (pathways.result, writeStages.result)
  }

  def constructJob(bp: Parameters): Job = {
    writeBuildManifest(bp)

    // build up stages
    val splitInput =
      splitSource(classOf[DocumentSource], getSplitParms(bp))

    val countDocs = FlowStage(classOf[ParserCounter])
    val offsetDocs = FlowStage(classOf[SplitOffsetter])

    // build up output of fork stage first
    val writeNames =
      FlowStage(classOf[DiskNameWriter], indexFileParms(bp, "names"))
    val writeNamesRev =
      FlowStage(classOf[DiskNameReverseWriter],
        indexFileParms(bp, "names.reverse"))
    val writeLengths =
      FlowStage(classOf[DiskLengthsWriter], indexFileParms(bp, "lengths"))
    val writeContent = if(bp.get("content", true)) {
      Some(FlowStage(classOf[PositionContentWriter],
        indexFileParms(bp, "content")))
    } else None

    val writeCorpus = if(bp.get("corpus", true)) {
      Some(FlowStage(classOf[SplitBTreeKeyWriter], bp.getMap("storeParams")))
    } else None

    // build up complicated fork stage
    val (stemmingBranches, stemmingWriters) = stemBranches(bp)
    val parseDocs = {
      val leadup = Seq(
        FlowStep(classOf[ParserSelector], bp.getMap("parser")),
        FlowStep(classOf[TagTokenizer], bp.getMap("tokenizer"))
      )

      val branches = stemmingBranches ++ Seq(
        extractor(classOf[FieldLengthExtractor], Some(writeLengths)),
        extractor(classOf[NumberedDocumentDataExtractor], Some(writeNames)),
        extractor(classOf[NumberedDocumentDataExtractor], Some(writeNamesRev)),

        //--- optional bits:
        extractor(classOf[ContentEncoder],
          writeContent,
          bp.getMap("tokenizer")),

        // enforce custom sort on CorpusFolderWriter;
        // it doesn't need a sort but is faster with one
        extractor(classOf[CorpusFolderWriter],
          writeCorpus,
          bp.getMap("storeParams"),
          Some(new KeyValuePair.KeyOrder()))
      ).flatten // remove anything that wasn't included in the build

      new FlowStage(
        FlowLinearStep(leadup ++ Seq(FlowMultiStep(branches))),
        gensym("ParseDocs")
      )
    }

    // DSL for when we don't want to create nodes explicitly
    // splitTo = each
    // joinTo = combined
    splitInput splitTo countDocs
    countDocs joinTo offsetDocs
    offsetDocs splitTo parseDocs

    // the rest of the stages will be joined automagically by sharing nodes
    // build up our set of stages
    val inputStages: Seq[FlowStage] =
      Seq(splitInput, countDocs, offsetDocs, parseDocs)
    val mustWriteStages: Seq[FlowStage] =
      Seq(writeNames, writeNamesRev, writeLengths)
    val mightWriteStages: Seq[FlowStage] =
      Seq(writeCorpus, writeContent).flatten

    val graph = inputStages ++
    mustWriteStages ++
    stemmingWriters ++
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
  }
}

