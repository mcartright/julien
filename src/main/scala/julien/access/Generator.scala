package julien
package access

import java.io.{BufferedReader,IOException}

trait Generator[T] extends Iterator[T] {
  protected def reader: BufferedReader
  def hasNext: Boolean = {
    val result = reader.ready
    if (!result) {
      try {
        reader.close
      } catch { case ioe: IOException => }
    }
    result
  }
}
