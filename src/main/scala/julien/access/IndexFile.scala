package julien
package access

import java.io.RandomAccessFile
import scala.collection.immutable.TreeMap

object IndexFile {
  case class ValueInfo(val start: Long, val length: Long)

  def apply(filename: String) = new IndexFile(filename)

  val byteOrdering = new math.Ordering[Array[Byte]] {
    def compare(x: Array[Byte], y: Array[Byte]): Int = {
      val sharedLength = math.min(x.length, y.length);
      var i = 0
      while (i < sharedLength) {
        val a = x(i)
        val b = y(i)
        if (a < b) return -1
        if (b < a) return 1
        i += 1
      }

      return x.length - y.length
    }
  }
}

class IndexFile private(val filename: String) {
  import IndexFile._

  // The underlying file
  val input = new RandomAccessFile(filename, "r")

  // Load the keys of this file
  private val _keys: TreeMap[Array[Byte], ValueInfo] = loadKeys

  // Standard methods
  def size: Int = _keys.size
  def keysIterator(): Iterator[Array[Byte]] = _keys.keysIterator

  def value(key: Array[Byte]): Array[Byte] = {
    val info = _keys(key)
    val data = Array.ofDim[Byte](info.length.toInt)
    input.synchronized {
      input.seek(info.start)
      input.readFully(data)
    }
    data
  }

  def streamValue(key: Array[Byte]): BufferedDataStream = {
    val info = _keys(key)
    BufferedDataStream(input, info.start, info.length)
  }

  // Assume the file pointers of the keys can fully fit in memory
  private def loadKeys: TreeMap[Array[Byte], ValueInfo] = {
    val fileLength = input.length
    val footerPosition = fileLength - 20 // 2 longs + 1 int
    val keyBuilder =
      TreeMap.newBuilder[Array[Byte], ValueInfo](byteOrdering)
    input.synchronized {
      input.seek(footerPosition)
      val dataSize = input.readLong
      val vocabSize = input.readLong
      val manifestSize = input.readLong

      // Now move to the vocab location and read it in
      input.seek(dataSize)
      val numKeys = VByte.uncompressLong(input)
      var i = 0
      while (i < numKeys) {
        val keySize = VByte.uncompressInt(input)
        val key = Array.ofDim[Byte](keySize)
        input.readFully(key)
        val start = VByte.uncompressLong(input)
        val length = VByte.uncompressLong(input)
        keyBuilder += (key -> ValueInfo(start,length))
        i += 1
      }
    }
    keyBuilder.result
  }
}
