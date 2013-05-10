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

package object flow {
  type Job = gt.execution.Job
  type Parameters = gt.Parameters
  type Order[T] = gt.Order[T]

  // ugly mutable state for gensym
  var count = 0
  def gensym(base: String="gensym"): String = {
    var output = base+count
    count += 1
    output
  }

  // look up inheritance tree to find Annotation
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

  def getClassOption(className: String): Option[Class[_]] = {
    try {
      Some(Class.forName(className))
    } catch {
      case _: Exception => None
    }
  }

  // Given a class, look up it's annotations to determine it's output type and ordering
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

  // Given a class and an ordering string, create an Order object
  def getOrder(typeClass: Class[_], order: Array[String]): Option[Order[_]] = {
    val obj = typeClass.newInstance
    if(!obj.isInstanceOf[Type[_]]) return None
    val orderObj = obj.asInstanceOf[Type[_]].getOrder(order : _*)
    if(orderObj == null) return None
    return Some(orderObj)
  }
}


