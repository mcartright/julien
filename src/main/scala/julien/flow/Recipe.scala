package julien.flow

import java.io.File
import collection.mutable.{ListBuffer,HashSet}
import julien.galago.tupleflow.Utility

// commonly used Recipes for new Flow interface
object Recipe {
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
      //println("forcing sort order of "+targetStage.name+" to be "+forceSort.get.getClass.getSimpleName)
      steps += FlowStep(forceSort.get)
    } else if(needsSort.nonEmpty) {
      //println("inferred sort order of "+targetStage.name+" as "+needsSort.get.getClass.getSimpleName)
      steps += FlowStep(needsSort.get)
    }

    // finish creating a pipe to the appropriate stage
    steps += FlowOutput(targetStage.makeInputNode(FlowNodeJoin(), forceSort))

    // turn into a branch
    Some(FlowLinearStep(steps.toSeq))
  }

  // for every index file we need to make parameters including at least its name
  def indexFileParms(bp: Parameters, indexName: String): Parameters = {
    val p = new Parameters
    p.set("filename", bp.getString("indexPath") + File.separator + indexName)
    p.set("skipping", bp.get("skipping", true))
    p.set("skipDistance", bp.get("skipDistance", 500L))
    p
  }

  def writeBuildManifest(bp: Parameters) {
    val indexPath = new File(bp.getString("indexPath")).getCanonicalPath
    val buildManifest = new File(indexPath, "buildManifest.json")

    Utility.makeParentDirectories(buildManifest)
    Utility.copyStringToFile(bp.toPrettyString, buildManifest)
  }
}

