package julien
package access

import java.io.BufferedReader
import scala.io.Source

case class Tweet(
  val id: String,
  val content: String,
  val user: String,
  val timestamp: Long,
  val source: String
)

class TwitterParser extends Function1[String, Iterator[Tweet]] {
  def apply(filename: String): Iterator[Tweet] =
    new TPIterator(Source.fromFile(filename).bufferedReader)

  class TPIterator(protected val reader: BufferedReader)
      extends Generator[Tweet]
      with ParseFunctions
  {
    def next: Tweet = {
      val line = reader.readLine()
      if (line == null)
        throw new Exception("Not EOF, but couldn't read line.")
      val parts = line.split('\t')
      if (parts.length != 4)
        throw new Exception(s"malformed tweet: ${line}")
      val id = s"${parts(0)}-${parts(1)}"
      Tweet(
        id = id,
        content = parts(2),
        user = parts(0),
        timestamp = parts(1).toLong,
        source = parts(3)
      )
    }
  }
}
