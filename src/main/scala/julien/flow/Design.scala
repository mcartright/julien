package julien
package flow

import julien.galago.tupleflow.Sorter

import collection.mutable.HashSet
import collection.JavaConversions._

// because I haven't used Scala's types as stringently as possible in FlowType
import scala.language.existentials

// A FlowType is used to describe the input and output types of
// any FlowStep or FlowStage. Note that stages must have orders
// and steps do not need to have an ordering.
case class FlowType(val clazz: Class[_], val order: Option[Order[_]]) {
  def className: String = clazz.getName
  def simpleName: String = clazz.getSimpleName
  override def toString = {
    var sb = new StringBuilder
    sb ++= simpleName
    if(order.nonEmpty) {
      sb ++= ":("
      sb ++= order.get.getOrderSpec.mkString(" ")
      sb ++= ")"
    }
    sb.result
  }

  def sorts: Seq[String] = {
    order match {
      case Some(obj) => obj.getOrderSpec.toSeq
      case None => Seq()
    }
  }

  def canPipeTo(other: FlowType): Boolean = {
    if(className != other.className) return false

    // make sure that all the sort requirements of the next stage
    // are present in this stage
    val inSort = sorts
    val outSort = other.sorts

    // other is more strict
    if(outSort.size > inSort.size) {
      false;
    } else if(outSort.size == inSort.size) {
      inSort.zip(outSort).forall {
        case (a, b) => a == b
      }
    } else { // if(inSort.size > outSort.size) {
      inSort.take(outSort.size).zip(outSort).forall {
        case (a, b) => a == b
      }
    }
  }
}

/** This trait is the enum for the type of stage connection
  * Tupleflow calls them each and combined, but I found split
  * and join easier to understand
  *
  * split breaks the outputs into multiple tasks (map in mapreduce)
  * join requires all the split inputs to be pulled
  * back together (reduce in mapreduce)
  */
trait FlowNodeKind
case class FlowNodeSplit() extends FlowNodeKind // splitTo, each
case class FlowNodeJoin() extends FlowNodeKind // joinTo, combined


/** This represents a node; it is used as an output for one Stage and
  * an input for another. If you are making these directly, you're doing
  * too much work.
  *
  * See FlowStage.makeInputNode, FlowStage.makeOutputNode,
  * FlowStage.joinTo, FlowStage.splitTo and FlowStage.pipeTo
  */
case class FlowNode(
  val flowType: FlowType,
  val kind: FlowNodeKind,
  val name: String = gensym("node")
)

/** FlowStage Factory. */
object FlowStage {
  def apply(
    clz: Class[_],
    parms: Parameters = new Parameters
  ) = new FlowStage(FlowStep(clz, parms), gensym(clz.getSimpleName))

  def apply(fs: AbstractFlowStep) = new FlowStage(fs)
}

/** A class representing knowledge about a particular stage.
  * Is mutable, so not a case class - see inputs and outputs HashSets
  */
class FlowStage(val step: AbstractFlowStep, val name: String=gensym("stage")) {
  // validate input, unfortunately not possible at compile time, afaik
  step match {
    case FlowInput(_) | FlowOutput(_) => {
      Console.err.println("Error: Stage has only FlowInput or FlowOutput node...")
      ???
    }
    case _ => { }
  }

  var inputs = {
    val init = allSteps.flatMap {
      case FlowInput(node) => Some(node)
      case _ => None
    }.toSeq
    HashSet[FlowNode](init:_*)
  }
  var outputs = {
    val init = allSteps.flatMap {
      case FlowOutput(node) => Some(node)
      case _ => None
    }.toSeq
    HashSet[FlowNode](init:_*)
  }

  // determine if this has explicitly specified FlowInput steps
  def hasInputSteps = allSteps.exists {
    case FlowInput(_) => true
    case _ => false
  }
  // determine if this has explicitly specified FlowOutput steps
  def hasOutputSteps = allSteps.exists {
    case FlowOutput(_) => true
    case _ => false
  }

  // walk our tree and construct a set of all the atomic steps we contain
  def allSteps: Set[AbstractFlowStep] = {
    def findAllSteps(
      start: AbstractFlowStep,
      accum: Set[AbstractFlowStep]
    ): Set[AbstractFlowStep] = start match {
      case lfs: FlowLinearStep =>
        accum ++ lfs.steps.map(grp => findAllSteps(grp, Set())).flatten
      case mfs: FlowMultiStep =>
        accum ++ mfs.steps.map(grp => findAllSteps(grp, Set())).flatten
      case fo: FlowOutput => accum ++ Set(fo)
      case fi: FlowInput => accum ++ Set(fi)
      case fs: FlowStep[_] => accum ++ Set(fs)
    }
    findAllSteps(step, Set())
  }

  // print out our steps in a lisp-like manner
  override def toString = {
    def getStepString(step: AbstractFlowStep): String = step match {
      case lfs: FlowLinearStep => {
        lfs.steps.map(getStepString(_)).mkString("(linear ", " ", ")")
      }
      case mfs: FlowMultiStep => {
        mfs.steps.map(getStepString(_)).mkString("(multi ", " ", ")")
      }
      case fs: FlowStep[_] => fs.action.getSimpleName
      case fo: FlowOutput => "*output*"
      case fi: FlowInput => "*input*"
    }
    getStepString(step)
  }

  // build up a list of all input and output types
  def inputTypes = inputs.map(_.flowType)
  def outputTypes = outputs.map(_.flowType)

  def inputSortOrder: Option[Order[_]] = step.inputType match {
    case Some(ft) => ft.order
    case None => None
  }

  //TODO better error messages here
  /** generate an input node on this stage, crashing if not possible */
  def makeInputNode(
    kind: FlowNodeJoin,
    orderOverride: Option[Order[_]] = None
  ) = {
    val flowType = step.inputType match {
      case Some(FlowType(clz, None)) => {
        if(orderOverride.isEmpty) {
          Console.err.println("Stage: "+this+" has an input type "+step.inputType+" that has no defined order. This is impossible in GalagoTupleflow. Dying.")
          ???
        } else {
          FlowType(clz, orderOverride)
        }
      }
      case Some(ft) => ft
      case None => {
        // TODO better errors
        ???
      }
    }

    val node = FlowNode(flowType, kind, gensym(name+"_input"))
    inputs += node
    node
  }

  /** Generate an input node on this stage, crashing if not possible */
  def makeOutputNode(
    kind: FlowNodeKind,
    orderOverride: Option[Order[_]] = None
  ) = {
    val flowType = step.outputType match {
      case Some(FlowType(clz, None)) => {
        if(orderOverride.isEmpty) {
          Console.err.println("Stage: "+this+" has an output type "+step.outputType+" that has no defined order. This is unpossible in GalagoTupleflow. Dying.")
          ???
        } else {
          FlowType(clz, orderOverride)
        }
      }
      case Some(ft) => ft
      case None => {
        // TODO better errors
        ???
      }
    }

    val node = FlowNode(flowType, kind, gensym(name+"_output"))
    outputs += node
    node
  }

  // convenience methods based around pipeTo
  def splitTo(other: FlowStage) = pipeTo(other, FlowNodeSplit())
  def joinTo(other: FlowStage) = pipeTo(other, FlowNodeJoin())

  // in order to pipe result of this stage to another, they must have the same type, and neither can be None
  def pipeTo(other: FlowStage, pipeKind: FlowNodeKind) = {
    val pipeName = gensym(name+"_"+other.name)

    if(step.outputType.isEmpty) {
      Console.err.println(step)
      Console.err.println(step.inputType)
      Console.err.println(step.outputType)
      Console.err.println("Unable to pipe from a stage with no output!")
      ???
    }
    if(other.step.inputType.isEmpty) {
      Console.err.println(other.step)
      Console.err.println(other.step.inputType)
      Console.err.println(other.step.outputType)
      Console.err.println("Unable to pipe to a stage with no output!")
      ???
    }

    val pipeType = {
      val inType = other.step.inputType.get
      val outType = step.outputType.get

      if(!outType.canPipeTo(inType)) {
        Console.err.println("Incompatible types in pipeTo: ")
        Console.err.println("  this stage offers:    "+outType)
        Console.err.println("  other stage requires: "+inType)
        ???
      }
      outType
    }

    val sharedPipe = FlowNode(pipeType, pipeKind, pipeName)

    outputs += sharedPipe
    other.inputs += sharedPipe
  }
}



trait AbstractFlowStep {
  def inputType: Option[FlowType]
  def outputType: Option[FlowType]
}

case class FlowInput(node: FlowNode) extends AbstractFlowStep {
  def inputType = Some(node.flowType)
  def outputType = Some(node.flowType)
}
case class FlowOutput(node: FlowNode) extends AbstractFlowStep {
  def inputType = Some(node.flowType)
  def outputType = Some(node.flowType)
}

object FlowStep {
  // if you construct a FlowStep on an order, it
  // generates a sort stage targeting that order :)
  def apply[T](sortOrder: Order[T]): FlowStep[Sorter[T]] = {
    val p = new Parameters
    p.set("class", sortOrder.getOrderedClass.getName)
    p.set("order", sortOrder.getOrderSpec.mkString(" "))
    FlowStep(classOf[Sorter[T]], p)
  }
}

case class FlowStep[T](
  val action: Class[T],
  val parms: Parameters = new Parameters(),
  val name: String=gensym("step"))
    extends AbstractFlowStep {
  def inputType = getInputType(action)
  def outputType = {
    // if this is a Sorter, it's type is dependent upon its parms,
    // not just its class;
    if(action == classOf[Sorter[T]]) {
      val dataClass = getClassOption(parms.get("class", "")).get
      val ordering = getOrder(dataClass, parms.get("order", "").split(" "))
      Some(FlowType(dataClass, ordering))
    } else {
      getOutputType(action)
    }
  }
}

/** A linear collection of steps; not explicit in tupleflow, but simpler here */
case class FlowLinearStep(val steps: Seq[AbstractFlowStep])
    extends AbstractFlowStep {
  def inputType = steps.head.inputType
  def outputType = steps.last.outputType
}

/** a set of steps to execute in parallel, each getting a copy of the input */
case class FlowMultiStep(val steps: Seq[AbstractFlowStep])
    extends AbstractFlowStep {
  private val inType = steps(0).inputType
  private val outType = steps(0).outputType

  // ensure that all of the parallel steps have the same input type
  steps.tail.foreach(step => {
    if(inType != None) {
      val firstType = inType.get
      val thisType = step.inputType.get
      //println(firstType+" in parallel with "+thisType)
      assert(firstType == thisType)
    } else {
      assert(step.inputType == inType)
    }
  })

  def inputType = steps(0).inputType
  def outputType = steps(0).outputType
}


