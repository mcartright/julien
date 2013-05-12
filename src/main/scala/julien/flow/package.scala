package julien

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
import gt.execution.{Job,InputStep,MultiStep,OutputStep, Stage, Step}

import collection.mutable.{ListBuffer,HashSet}
import collection.JavaConversions._

/** The flow package facilitates creation of Tupleflow jobs. Tupleflow itself
  * is a suitable software package for distributed computation, however the API
  * is not very pleasant, making the generation of new Tupleflow jobs painful.
  */
package object flow {
  type Job = gt.execution.Job
  type Parameters = gt.Parameters
  type Order[T] = gt.Order[T]

  // ugly mutable state for gensym - don't let anyone see it.
  private var count = 0

  /** Function to generate a unique id for connecting jobs together.
    * A prefix can be supplied if desired.
    */
  def gensym(base: String="gensym"): String = {
    var output = base+count
    count += 1
    output
  }

  /** Returns all of the Order classes for a given Tupleflow type.
    */
  def getOrders(typeClass: Class[_]) : Array[Class[Order[_]]] = {
    val memberClasses = typeClass.getDeclaredClasses
    val filtered = memberClasses.filter { mc =>
      // For each member class/interface, we see what interfaces it implements,
      // and pick ones that implement the Order interface
      val interfaces = mc.getInterfaces
      interfaces.exists(i => i == classOf[Order[_]])
    }
    return filtered.map(_.asInstanceOf[Class[Order[_]]])
  }

  /** Hunts for a particular annotation attached to a class. If found,
    * returns an option filled w/ the annotation. Otherwise returns None.
    */
  def findAnnotation[T <: java.lang.annotation.Annotation](
    objClass: Class[_],
    annClass: Class[T]
  ): Option[T] = {
    var cur = objClass
    while(cur != null) {
      if(cur.isAnnotationPresent(annClass)) {
        val ann = cur.getAnnotation(annClass)
        if(ann != null && ann.isInstanceOf[T]) {
          return Some(ann.asInstanceOf[T])
        }
      }
      cur = cur.getSuperclass
    }
    None
  }

  /** Optionally returns the input type of a Tupleflow step.
    * We may return None, if no input class
    * is defined (e.g. a source step), and even in the returned FlowType,
    * we may not have an Order defined if the class is an internal
    * step.
    */
  def getInputType(stepClass: Class[_]): Option[FlowType] = {
    val ic = findAnnotation(stepClass, classOf[InputClass]) match {
      case None => return None
      case Some(ann) => ann.asInstanceOf[InputClass]
    }

    val inputClass = getClassOption(ic.className) match {
      case None => return None
      case Some(clz) => clz
    }

    val order = getOrder(inputClass, ic.order)
    Some(FlowType(inputClass, order))
  }

  /** Optionally returns a class for the given string. */
  def getClassOption(className: String): Option[Class[_]] = {
    try {
      Some(Class.forName(className))
    } catch {
      case _: Exception => None
    }
  }

  /** Given a class, look up it's annotations to determine it's
    * output type and order. We may return None, if no output class
    * is defined (e.g. a writer step), and even in the returned FlowType,
    * we may not have an Order defined if the class is an internal
    * step.
    */
  def getOutputType(stepClass: Class[_]): Option[FlowType] = {
    val oc = findAnnotation(stepClass,classOf[OutputClass]) match {
      case None => return None
      case Some(ann) => ann.asInstanceOf[OutputClass]
    }

    val outputClass = getClassOption(oc.className) match {
      case None => return None
      case Some(clz) => clz
    }

    val order = getOrder(outputClass, oc.order)
    Some(FlowType(outputClass, order))
  }

  /** Given a class and an ordering string, tries to create an Order object
    * that is an actual Order of the given type. If the given order does not
    * exist, returns None.
    */
  def getOrder(typeClass: Class[_], order: Array[String]): Option[Order[_]] = {
    val obj = typeClass.newInstance
    if(!obj.isInstanceOf[Type[_]]) return None
    val orderObj = obj.asInstanceOf[Type[_]].getOrder(order : _*)
    if(orderObj == null) return None
    return Some(orderObj)
  }
}


