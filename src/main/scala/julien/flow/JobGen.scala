package julien
package flow

// Aliasing for shorthand
import julien.galago.{core => gcore, tupleflow => gt}

import java.io.{File, PrintStream}
import gcore.index.corpus._
import gcore.index.disk._
import gcore.util.BuildStageTemplates._
import gcore.parse._
import gcore.types._
import gt._
import gt.execution.{ConnectionAssignmentType, Verification, ErrorStore}
import gt.execution.{InputStep,MultiStep,OutputStep, Stage, Step}

import collection.mutable.{ListBuffer,HashSet}
import collection.JavaConversions._


object JobGen {
  // given a set of stages, and a start stage, find all successors
  def successors(startStage: FlowStage, stages: Set[FlowStage]): Set[FlowStage] = {
    val outPipes = startStage.outputs

    outPipes.map(pipe => {
      stages.toSet.filter(_.inputs.contains(pipe))
    }).flatten.toSet
  }

  // pretty-print the type of each stage flowing together
  def printTypeGraph(startStage: FlowStage, stages: Set[FlowStage], indent: String="") {
    if(startStage.inputTypes.nonEmpty) {
      print(indent+startStage.inputTypes.mkString(", "))
    } else {
      print(indent+"START")
    }
    if(startStage.outputTypes.nonEmpty) {
      println(" => ")
      successors(startStage, stages).foreach(next => {
        printTypeGraph(next, stages, indent + "  ")
      })
    } else {
      println(" =>\n"+indent+"  DONE")
    }
  }

  // build a sequence of Tupleflow steps from FlowSteps
  //  called publicly from create
  private def seqFromFlowStep(step: AbstractFlowStep): Seq[Step] = {
    step match {
      case FlowInput(node) => Seq(new InputStep(node.name))
      case FlowOutput(node) => Seq(new OutputStep(node.name))
      case FlowLinearStep(steps) => {
        // Tupleflow doesn't have our concept of FlowLinearSteps
        steps.map(lstep => seqFromFlowStep(lstep)).reduce(_ ++ _)
      }
      case FlowMultiStep(groups) => {
        val fork = new MultiStep()
        groups.foreach(grp => {
          fork.addGroup(seqFromFlowStep(grp))
          //fork.addGroup(gensym("group"), seqFromFlowStep(grp))
        })
        Seq(fork)
      }
      case FlowStep(clz, parms, _) => {
        Seq(new Step(clz, parms))
      }
    }
  }

  /** Build a Tupleflow Stage from a Flow Stage
    * called publicly from create.
    */
  private def stageFromFlowStage(
    fs: FlowStage,
    graph: Seq[FlowStage]
  ): Stage = {
    val stage = new Stage(fs.name)

    // add all inputs, generating input steps conditionally
    fs.inputs.foreach(input => {
      stage.addInput(input.name, input.flowType.order.get)
      if(!fs.hasInputSteps) {
        stage.add(new InputStep(input.name))
      }
    })

    // add all steps
    seqFromFlowStep(fs.step).foreach(stage.add(_))

    // add all outputs, generating OutputSteps conditionally
    fs.outputs.foreach(output => {
      stage.addOutput(output.name, output.flowType.order.get)
      if(!fs.hasOutputSteps) {
        stage.add(new OutputStep(output.name))
      }
    })

    stage
  }

  def create(graph: Seq[FlowStage]): Job = {
    val job = new Job()

    // generate each stage
    graph.foreach(flowStage => {
      job.add(stageFromFlowStage(flowStage, graph))
    })

    // now hook together with either join or combine nodes as appropriate...
    // use the logic that everything without an input is a start stage
    graph.foreach(srcStage => {
      srcStage.outputs.foreach(node => {
        // find the corresponding input nodes
        val targetStages = graph.filter(_.inputs.contains(node))
        targetStages.foreach(tgtStage => {
          node.kind match {
            case FlowNodeSplit() => job.each(srcStage.name, tgtStage.name)
            case FlowNodeJoin() => job.combined(srcStage.name, tgtStage.name)
          }
        })
      })
    })

    job
  }

  def createAndVerify(graph: Seq[FlowStage]): Job = {
    val job = create(graph)
    val store = new ErrorStore
    Verification.verify(job, store)
    if(store.hasStatements) {
      Console.err.println(store)
      ???
    }
    job
  }
}

