package julien
package access

import java.io.{IOException,BufferedReader}
import scala.io.Source

case class SimpleWebDoc(val id: String, val url: String, val content: String)

object TrecWebParser {
  val docOpen = "<DOC>"
  val docClose = "</DOC>"
  val idOpen = "<DOCNO>"
  val idClose = "</DOCNO>"
  val hdrOpen = "<DOCHDR>"
  val hdrClose = "</DOCHDR>"
  val idPattern = s"""${idOpen}(.+)${idClose}""".r
}

class TrecWebParser extends Function1[String, Iterator[SimpleWebDoc]] {
  import TrecWebParser._

  def apply(filename: String): Iterator[SimpleWebDoc] =
    new TWPIterator(Source.fromFile(filename).bufferedReader)

  class TWPIterator(protected val reader: BufferedReader)
      extends Generator[SimpleWebDoc]
      with ParseFunctions
  {
    def next: SimpleWebDoc = {
      if (None == waitFor(docOpen))
        throw new Exception("Had no next or has malformed document.")

      val id = waitFor(idOpen) match {
        case Some(line) => idPattern.findFirstMatchIn(line).get.group(1)
        case None => throw new Exception("No document id provided.")
      }
      waitFor(hdrOpen)
      val url = readUrl
      waitFor(hdrClose)

      val builder = new StringBuilder(20 * 1024)
      var line = reader.readLine
      while (line != null) {
        if (line.startsWith(docClose))
          return SimpleWebDoc(id, url, builder.result)
        builder.append(line)
        builder.append('\n')
        line = reader.readLine
      }
      throw new Exception("Did not find close tag for document")
    }
  }
}
