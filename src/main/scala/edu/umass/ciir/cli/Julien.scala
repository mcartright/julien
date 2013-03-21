package edu.umass.ciir.cli

object Julien {
  def main(argv: Array[String]) : Unit = {
    Console.printf("Julien CLI v0.0.1.\n")
    Console.printf("Args: %s\n", argv.mkString(","))
  }
}
