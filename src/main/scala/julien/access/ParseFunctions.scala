package julien
package access

import java.io.BufferedReader

/** Encapsulates a few simple functions to aid in parsing. */
trait ParseFunctions {
  /** Accessor for the reader used to provide data. Usually overriden as a val.
    */
  protected def reader: BufferedReader

  /** Returns the first line that starts with the supplied tag. */
  protected def waitFor(tag: String): Option[String] = {
    var line = reader.readLine()
    while (line != null) {
      if (line.startsWith(tag)) return Some(line)
    }
    return None
  }

  protected def scrubUrl(url: String): String = {
    var scrubbed =  if (url.last == '#') url.init else url
    while (scrubbed.last == '/') scrubbed = scrubbed.init
    scrubbed.replace(":80", "")
  }

  protected def readUrl: String = {
    val url = reader.readLine
    val space = url.indexOf(' ')
    scrubUrl(url.substring(0, space))
  }
}
