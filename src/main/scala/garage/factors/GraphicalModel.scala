package garage
package factors

case class Query(text: String, number: String)

object GraphicalModel {
  def display(generator: RandomVariableGenerator) : Unit = Console.println(generator)
}


