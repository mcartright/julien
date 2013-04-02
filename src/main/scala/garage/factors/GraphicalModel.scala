package factors

import org.lemurproject.galago.core.index.disk.DiskIndex
import org.lemurproject.galago.core.index.Index

import scala.collection.immutable.List

case class Query(text: String, number: String)

object GraphicalModel {
  def display(generator: RandomVariableGenerator) : Unit = Console.println(generator)
}


