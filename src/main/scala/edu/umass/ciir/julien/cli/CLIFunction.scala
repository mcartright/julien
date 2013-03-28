package edu.umass.ciir.julien.cli

import org.lemurproject.galago.tupleflow.Parameters
import java.io.PrintStream

trait CLIFunction {
  def name: String
  def help: String
  def run(p: Parameters, out: PrintStream) : Unit
  def run(argv: Array[String], out: PrintStream) : Unit = argv.length match {
    case 0 => out.println(help)
    case _ => run(new Parameters(argv), out)
  }
}
