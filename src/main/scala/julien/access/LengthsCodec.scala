package julien
package access

import java.io.{DataInput, DataOutput}

class LengthsCodec extends Codec[Int] {
  // Going to try loading these into memory,
  // skips just get in the way in most cases
  override def useSkips: Boolean = false

  // Header Info
  var numEntries = 0
  var max = 0

  override def setKey(newKey: Array[Byte]) {
    numEntries = 0
    max = 0
  }

  override def writeHeader(out: DataOutput) {
    out.writeInt(numEntries)
    out.writeInt(max)
  }

  override def readHeader(in: DataInput) {
    numEntries = in.readInt
    max = in.readInt
  }

  def encode(i: Int, out: DataOutput) {
    out.writeInt(i)
    numEntries += 1
    max = math.max(max, i)
  }

  def decode(in: DataInput): Int = in.readInt
}
