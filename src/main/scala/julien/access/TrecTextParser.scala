package julien
package access

import java.io.{EOFException,IOException,BufferedReader}
import scala.io.Source

case class SimpleDoc(val id: String, val content: String)

object TrecTextParser {
  def apply = new TrecTextParser()
  val starts = Array("<TEXT>", "<HEADLINE>", "<TITLE>", "<HL>", "<HEAD>",
    "<TTL>", "<DD>", "<DATE>", "<LP>", "<LEADPARA>")
  val ends = Array("</TEXT>", "</HEADLINE>", "</TITLE>", "</HL>", "</HEAD>",
    "</TTL>", "</DD>", "</DATE>", "</LP>", "</LEADPARA>")
  val docOpen = "<DOC>"
  val docClose = "</DOC>"
  val idOpen = "<DOCNO>"
  val idClose = "</DOCNO>"
}

class TrecTextParser extends Function1[String, Iterator[SimpleDoc]] {
  import TrecTextParser._

  def apply(filename: String): Iterator[SimpleDoc] =
    new TTPIterator(Source.fromFile(filename).bufferedReader)

  class TTPIterator(protected val reader: BufferedReader)
      extends Generator[SimpleDoc]
      with ParseFunctions
  {
    private val docnoPat = s"""${docOpen}(.+)${docClose}""".r
    def next: SimpleDoc = {
      if (reader == null || None == waitFor(docOpen))
        throw new Exception("Had no next or had malformed document.")
      val id = parseDocNumber()
      if (id.isEmpty) throw new Exception("DOC opened, but no DOCNO.")
      val builder = new StringBuilder()
      var inTag: Int = -1
      var line: String = reader.readLine()
      while (line != null) {
        if (line.startsWith(docClose))
          return SimpleDoc(id.get, builder.result)

        if (line.startsWith("<")) {
          if (inTag >= 0 && line.startsWith(ends(inTag))) {
            inTag = -1
            builder.append(line)
            builder.append('\n')
          } else if (inTag < 0) {
            for (i <- 0 until starts.length; if line.startsWith(starts(i)))
              inTag = i
          }
        }

        if (inTag >= 0) {
          builder.append(line)
          builder.append('\n')
        }
        line = reader.readLine
      }
      SimpleDoc(id.get, builder.result)
    }

    private def parseDocNumber(): Option[String] = {
      val header = waitFor(idOpen)
      if (header.isEmpty) return None
      val builder = new StringBuilder(header.get)
      while (!builder.contains(idClose)) {
        val line = reader.readLine()
        if (line == null)
          throw new EOFException("Did not find DOCNO close tag.")
        builder ++= line
      }
      val text = builder.result
      Some(docnoPat.findFirstMatchIn(text).get.group(1).trim)
    }
  }
}
