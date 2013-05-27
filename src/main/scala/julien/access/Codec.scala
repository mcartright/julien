package julien
package access

import java.io.{DataInput, DataOutput}

trait Codec[@specialized(Int, Long) T] {
  def useSkips: Boolean = true
  def encode(obj: T, out: DataOutput)
  def decode(in: DataInput): T
  def writeHeader(out: DataOutput) {}
  def readHeader(in: DataInput) {}
  def state: Array[Byte]
  def state_=(bytes: Array[Byte])
}
