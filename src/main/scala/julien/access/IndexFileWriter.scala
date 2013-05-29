package julien
package access

import java.io.{DataOutputStream, FileOutputStream}
import scala.collection.immutable.TreeMap
import IndexFile.ValueInfo
import julien.galago.tupleflow.Utility

class IndexFileWriter[@specialized(Int,Long) T](
  val filename: String,
  val codec: Codec[T]
) {

  // Inner class used to help get the job done
  // This maintains skips for a single codec-encoded list
  class IndexListWriter(
    val key: Array[Byte],
    out: DataOutputStream,
    codec: Codec[T]
  ) {
    val start = channel.position

    def append(data: T) {
      // will worry about skips later
      codec.encode(data, out)
    }

    def close {
      // nothing yet
    }
  }

  // Outer class members
  private val fileStream = new FileOutputStream(filename)
  private val channel = fileStream.getChannel
  private val dataStream = new DataOutputStream(fileStream)

  private var keys = TreeMap[Array[Byte], ValueInfo]()(IndexFile.byteOrdering)
  private var listWriter: IndexListWriter = null
  private var currentKey: Array[Byte] = Array.empty[Byte]

  def append(key: Array[Byte], data: T) {
    val result = IndexFile.byteOrdering.compare(currentKey, key)
    assert (result <= 0,
      "Keys must be received in ascending order. " +
        s"at: ${Utility.toString(currentKey)}; Got ${Utility.toString(key)}")

    if (result < 0 || currentKey.length == 0) {
      if (currentKey.length > 0) closeCurrentList
      openNewList(key)
      currentKey = key
    }
    listWriter.append(data)
  }

  private def openNewList(key: Array[Byte]) {
    // In case it's maintaining state per-key
    codec.setKey(key)
    listWriter = new IndexListWriter(key, dataStream, codec)
  }

  private def closeCurrentList {
    listWriter.close
    val position = channel.position
    val info = ValueInfo(listWriter.start, position)
    keys = keys + (listWriter.key -> info)
  }

  def close {
    if (currentKey.length > 0) closeCurrentList

    // Note size b/c of data
    val dataSize = channel.position

    // Now add keys
    writeKeys
    val keySize = channel.position

    // Eventually add a manifest
    val manifestSize = 0

    dataStream.writeLong(dataSize)
    dataStream.writeLong(keySize)
    dataStream.writeInt(manifestSize)
    dataStream.close
  }

  private def writeKeys {
    // start with number of keys
    VByte.compressLong(keys.size.toLong, dataStream)
    val iterator = keys.iterator
    while (iterator.hasNext) {
      val (key, info) = iterator.next
      VByte.compressInt(key.length, dataStream)
      dataStream.write(key)
      VByte.compressLong(info.start, dataStream)
      VByte.compressLong(info.length, dataStream)
    }
  }
}
