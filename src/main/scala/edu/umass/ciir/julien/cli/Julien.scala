package edu.umass.ciir.julien.cli

import java.io.PrintStream
import org.slf4j.{Logger,LoggerFactory}
import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

object Julien {
  val log = LoggerFactory.getLogger("Julien")
  def main(argv: Array[String]) : Unit = run(argv)

  def run(argv: Array[String], out : PrintStream = Console.out) : Unit = {
    // Use experimental reflection capabilities to find all CLIFunctions
    val names = findApps().map(a => (a.name, a)).toMap
    Console.printf("Found apps: %s\n", names.keys.mkString(","))
    val (fnName: String, remaining: Array[String]) = (argv.head, argv.tail)
    if (names.contains(fnName)) {
      names(fnName).run(remaining, out)
    } else {
      out.printf("Function '%s' was not found. Options: \n%s\n",
        fnName, names.keys.toList.sorted.mkString(","))
    }
  }

  def findApps() : Iterable[CLIFunction] = {
    val mirror = runtimeMirror(getClass.getClassLoader)
    val cliPkg = mirror.staticPackage("edu.umass.ciir.julien.cli")
    Console.printf("Looking at pkg symbol: %s\n", cliPkg)
    val apps = cliPkg.typeSignature.members.filter { s =>
      // Anything that is a subtype of CLIFuction
      Console.printf("testing symbol '%s' : %b\n",
        s, s.typeSignature <:< typeOf[CLIFunction])
      s.typeSignature <:< typeOf[CLIFunction]
    } map { sym =>
      // take that symbol (as a ModuleSymbol), and get the object instance
      mirror.reflectModule(sym.asModule).instance.asInstanceOf[CLIFunction]
    }
    apps
  }
}
