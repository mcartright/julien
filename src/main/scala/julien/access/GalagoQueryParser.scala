package julien
package access

import scala.util.parsing.combinator.JavaTokenParsers

object GalagoQueryParser {
  val parser = new GalagoQueryParser

  def apply(input: String): String = {
    val result = parser.parseAll(parser.node, input)
    result.get.print()
  }
}

/**
  * Currently parses:
  * query = node
  * node = '#' nodeName (':' pair)* '(' args ')'
  * nodeName = "combine" | "seqdep" | "sdm"
  * pair = string '=' string
  * args = node+ | string+
  */
class GalagoQueryParser extends JavaTokenParsers {

  def node: Parser[Node] = sdm | combine

  def sdm: Parser[Node] = (("#seqdep"|"#sdm")~"(")~>strings<~")" ^^ { l =>
    SDM(l.mkString(" "))
  }

  def combine: Parser[Node] =
    ("#combine" ~> params <~ "(") ~ (nodes | strings) <~ ")" ^^ {
      case weights~nodes => {
        val weighted = if (nodes.head.isInstanceOf[String]) {
          nodes.zip(weights).map { case (t, w) =>
              val term = Term(t.toString)
              term.weight = w
              term
          }
        } else {
          nodes.zip(weights).map { case (obj, w) =>
              val n = obj.asInstanceOf[Node]
              n.weight = w
              n
          }
        }
        Combine(weighted.asInstanceOf[List[Node]])
      }
    }

  def nodes: Parser[List[Node]] = rep1(node)
  def strings: Parser[List[String]] = rep1(string)
  def params: Parser[List[Double]] = rep(":"~>pair) ^^ {
    _.sortBy(_._1).map(_._2)
  }

 def pair: Parser[Tuple2[Int,Double]] =
    (wholeNumber <~ "=") ~ floatingPointNumber ^^ {
      case k~v => (k.toInt -> v.toDouble)
    }

  def string: Parser[String] = """\w+""".r

  trait Node {
    def print(indent: String = ""): String
    var weight: Double = 1.0
  }
  case class Term(t: String) extends Node {
    def print(indent: String) =
      s"""${indent}Dirichlet(Term("${t}"), IndexLengths())"""
  }
  case class SDM(phrase: String) extends Node {
    def print(indent: String) =
      s"""${indent}sdm("${phrase}", weight = ${weight})"""
  }
  case class Combine(nodes: List[Node]) extends Node {
    def print(indent: String) = {
      val newIndent = indent + "  "
      val children = nodes.map(_.print(newIndent)).mkString(",\n")
      s"""${indent}Combine(weight = ${weight},
${children}
${indent})"""
    }
  }
}
