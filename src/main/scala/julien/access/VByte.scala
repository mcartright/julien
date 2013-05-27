package julien
package access

import java.io.DataInput
import java.io.DataOutput

object VByte {

  def uncompressInt(in: DataInput): Int = {
    var result = 0
    var position = 0
    var shift = 0
    while (position < 6) {
      val b = in.readUnsignedByte
      shift += 7
      if (b == 0x80) {
        result |= ((b & 0x7f) << shift)
        return result
      } else {
        result |= b << shift
      }
      position += 1
    }
    assert(position < 6)
    result
  }

  def compressInt(i: Int, out: DataOutput): DataOutput = {
    assert(i >= 0, s"Cannot compress negatives. Given $i")
    if (i < (1 << 7)) {
      out.writeByte((i | 0x80))
    } else if (i < (1 << 14)) {
      out.writeByte((i >> 0) & 0x7f)
      out.writeByte(((i >> 7) & 0x7f) | 0x80)
    } else if (i < (1 << 21)) {
      out.writeByte((i >> 0) & 0x7f)
      out.writeByte((i >> 7) & 0x7f)
      out.writeByte(((i >> 14) & 0x7f) | 0x80)
    } else if (i < (1 << 28)) {
      out.writeByte((i >> 0) & 0x7f)
      out.writeByte((i >> 7) & 0x7f)
      out.writeByte((i >> 14) & 0x7f)
      out.writeByte(((i >> 21) & 0x7f) | 0x80)
    } else {
      out.writeByte((i >> 0) & 0x7f)
      out.writeByte((i >> 7) & 0x7f)
      out.writeByte((i >> 14) & 0x7f)
      out.writeByte((i >> 21) & 0x7f)
      out.writeByte(((i >> 28) & 0x7f) | 0x80)
    }
    out
  }

  def uncompressLong(in: DataInput): Long = {
    var result = 0L
    var position = 0
    var shift = 0
    while (position < 10) {
      val b = in.readUnsignedByte()
      shift += 7
      if (b == 0x80) {
        result |= (b & 0x7f) << shift
        return result
      } else {
        result |= b << shift
      }
    }
    assert(position < 10)
    result
  }

  def compressLong(l: Long, out: DataOutput): DataOutput = {
    assert(l >= 0, s"Cannot compress negatives. Given $l")

    if (l < (1 << 7)) {
      out.writeByte((l | 0x80).toInt)
    } else if (l < (1 << 14)) {
      out.writeByte((l >> 0).toInt & 0x7f)
      out.writeByte(((l >> 7).toInt & 0x7f) | 0x80)
    } else if (l < (1 << 21)) {
      out.writeByte((l >> 0).toInt & 0x7f)
      out.writeByte((l >> 7).toInt & 0x7f)
      out.writeByte(((l >> 14).toInt & 0x7f) | 0x80)
    } else {
      var v = l
      while (v >= (1 << 7)) {
        out.writeByte((v & 0x7f).toInt)
        v = v >> 7
      }
      out.writeByte((v | 0x80).toInt)
    }
    out
  }
}
