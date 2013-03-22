package edu.umass.ciir.julien.cli

import edu.umass.ciir.julien._

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.tree._
import java.io.{InputStream,FileInputStream}

object Julien {
  def main(argv: Array[String]) : Unit = {
    if (argv.size == 0) {
      Console.println("Received no file to interpret")
      return
    }

    val inputFile = argv(0)
    val antlrInStream = new ANTLRInputStream(new FileInputStream(inputFile))
    val lexer = new JulienLexer(antlrInStream)
    val tokens = new CommonTokenStream(lexer)
    val parser = new JulienParser(tokens)
    parser.setBuildParseTree(true)
    val tree = parser.stmt()
    Console.println(tree.toStringTree(parser));

    // Hook this up to our environment
    val walker = new ParseTreeWalker()
    val env = new QueryBuilder()
    walker.walk(env, tree)
  }
}
