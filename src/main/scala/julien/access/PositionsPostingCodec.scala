package julien
package access

import java.io._
import julien.galago.tupleflow.Utility
import julien.galago.core.util.ExtentArray


class PositionsPostingCodec extends Codec[PositionsPosting] {
  val byteOrder = IndexFile.byteOrdering

  // This is all the state we need
  var lastDocid = 0

  // This is header information
  var numPostings = 0
  var numPositions = 0L
  var maxPositionsInPosting = 0

  override def setKey(newKey: Array[Byte]) {
    lastDocid = 0
  }

  def encode(p: PositionsPosting, out: DataOutput) {
    // d-gap docids
    VByte.compressInt(p.docid - lastDocid, out)
    lastDocid = p.docid

    VByte.compressInt(p.count, out)

    // d-gap positions
    var lastPos = 0
    var i = 0
    while (i < p.count) {
      VByte.compressInt(p.positions.begin(i) - lastPos, out)
      lastPos = p.positions.begin(i)
      i += 1
    }

    // Update header information
    numPostings += 1
    numPositions += p.count
    maxPositionsInPosting = math.max(maxPositionsInPosting, p.count)
  }

  private val thePosting = PositionsPosting("",0,0,new ExtentArray)
  def decode(in: DataInput): PositionsPosting = {
    // d-gapping, so add
    thePosting.docid = VByte.uncompressInt(in) + lastDocid
    lastDocid = thePosting.docid

    thePosting.count = VByte.uncompressInt(in)

    // positions are also d-gapped
    thePosting.positions.clear
    var i = 0
    var lastPos = 0
    while (i < thePosting.count) {
      val pos = VByte.uncompressInt(in) + lastPos
      lastPos = pos
      thePosting.positions.add(pos)
      i += 1
    }
    thePosting
  }

  override def writeHeader(out: DataOutput) {
    out.writeInt(numPostings)
    out.writeLong(numPositions)
    out.writeInt(maxPositionsInPosting)
  }

  override def readHeader(in: DataInput) {
    numPostings = in.readInt
    numPositions = in.readLong
    maxPositionsInPosting = in.readInt
  }

  override def state: Array[Byte] = Utility.fromInt(lastDocid)
  override def state_=(bytes: Array[Byte]) = lastDocid = Utility.toInt(bytes)
}
