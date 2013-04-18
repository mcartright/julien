package julien
package config

case class ParserParams(
  filetype: Option[String] = None,
  externalParsers: List[ExtParserParams] = List[ExtParserParams]()
) extends StandardParameters

case class ExtParserParams(
  filetype: String,
  class: String
) extends StandardParameters
