package julien.flow

import java.io.File
import collection.mutable.{ListBuffer,HashSet}
import julien.galago.tupleflow.Utility

/** Commonly used recipes (i.e. patterns) in building Tupleflow
  * jobs.
  */
object Recipe {

  /** Adds a step -> optional sort -> output step combination. Mainly used when
    * repeating outputs from a Tupleflow MultiStep.
    */
  def extractor(
        extractor: Class[_],
        output: Option[FlowStage],
        extractParms: Parameters = new Parameters,
        forceSort: Option[Order[_]] = None): Option[FlowLinearStep] = {
    // don't do anything if there's no output stage to hook to
    if(output.isEmpty) return None

    // grab pieces of target stage which are interesting
    val targetStage = output.get
    val needsSort = targetStage.inputSortOrder

    // build a list of steps
    var steps = ListBuffer[AbstractFlowStep]()

    // do the extraction step
    steps += FlowStep(extractor, extractParms)

    // infer the necessary sort if not forced
    if(forceSort.nonEmpty) {
      steps += FlowStep(forceSort.get)
    } else if(needsSort.nonEmpty) {
      steps += FlowStep(needsSort.get)
    }

    // finish creating a pipe to the appropriate stage
    steps += FlowOutput(targetStage.makeInputNode(FlowNodeJoin(), forceSort))

    // turn into a branch
    Some(FlowLinearStep(steps.toSeq))
  }

  /** Constructs default parameters for an index file writer.
    * This method needs refactoring, as it adds more than is necesary
    * for most writers.
    */
  def indexFileParms(bp: Parameters, indexName: String): Parameters = {
    val p = new Parameters
    p.set("filename", bp.getString("indexPath") + File.separator + indexName)
    p.set("skipping", bp.get("skipping", true))
    p.set("skipDistance", bp.get("skipDistance", 500L))
    p
  }

  /** Writes a build manifest to the location indicated in the provided
    * parameters.
    */
  def writeBuildManifest(bp: Parameters) {
    val indexPath = new File(bp.getString("indexPath")).getCanonicalPath
    val buildManifest = new File(indexPath, "buildManifest.json")

    Utility.makeParentDirectories(buildManifest)
    Utility.copyStringToFile(bp.toPrettyString, buildManifest)
  }
}

