package julien
package config

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

trait StandardParameters {
  override def toString = toJson()
  def toJson(p:String = ""): String = {
    val b = ListBuffer[String]()
    b += s"""$p"_name" : "${getClass.getName}" """
    for (field <- getClass.getDeclaredFields) {
      val lead = """$p${field.getName}""""
      field.get(this) match {
        case Byte | Short | Int =>
          b += s"""$lead : ${field.getInt(this)}"""
        case Float => b += s"""$lead : ${field.getFloat(this)}"""
        case Double => b += s"""$lead : ${field.getDouble(this)}"""
        case Long => b += s"""$lead : ${field.getLong(this)}"""
        case Boolean => b +=  s"""$lead : ${field.getBoolean(this)}"""
        case s: String => b += s"""$lead : $s"""
        case sp: StandardParameters => {
          val inner = sp.toJson(p+" ")
          b += s"""$lead : $inner"""
        }
        case l: List[_] => {
          val prepped = l.map { item =>
            item match {
              case s: String => s""" "$s" """.trim
              case _ => item.toString
            }
          }
          val inner = prepped.mkString("[", "," ,"]")
          b += s"""$lead : $inner"""
        }
      }
    }

    // render
    val inner = b.map(_.trim).mkString(",\n")
    s"$p{\n$inner\n$p}"
  }
}
