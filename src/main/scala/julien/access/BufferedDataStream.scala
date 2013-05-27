package julien
package access

import java.io.{EOFException, RandomAccessFile}

object BufferedDataStream {
  def apply(raf: RandomAccessFile, s: Long, l: Long) =
    new BufferedDataStream(raf, s, l)
}

class BufferedDataStream private(
  private[this] val input: RandomAccessFile,
  private[this] val start: Long,
  val length: Long // this one is public
) {
  // Probably should be global - largest cache size
  private[this] val maxCacheLength = 32768

  // The first absolute position that is invalid to us
  private[this] val end = start + length

  // Buffer variables

  // Absolute start of buffered contents
  // Notice that this is always >= start and only monotonically goes up
  private[this] var bufferStart = start

  // Relative position in the buffered contents
  private[this] var bufferPosition: Int = 0

  // How many bytes are valid in the cache?
  private[this] var bufferLength: Int = 0

  // We make this once, and use 1 extra variable to track the
  // "active" size of it. Saves on making new buffers over and over
  private[this] val cacheBuffer = Array.ofDim[Byte](maxCacheLength)

  // Public Methods
  def absolutePosition: Long = bufferStart + bufferPosition
  def isDone: Boolean = end <= absolutePosition
  def position: Long = absolutePosition - start

  /** Relative positional seek, i.e. seek(0) is the start of this buffer. */
  def seek(offset: Long) = seekAbsolute(start + offset)

  // Next three functions may not use the cache depending on the
  // size requested
  def readFully(buf: Array[Byte]) {
    if (buf.length <= maxCacheLength) {
      cache(buf.length)
      System.arraycopy(cacheBuffer, bufferPosition, buf, 0, buf.length)
      update(buf.length)
    } else {
      // bypass caching
      if (bufferStart + bufferPosition + buf.length > end) crap(buf.length)
      input.synchronized {
        input.seek(bufferStart + bufferPosition)
        input.readFully(buf)
      }
      invalidateOver(buf.length)
    }
  }

  def readFully(buf: Array[Byte], offset: Int, len: Int) {
    if (len < maxCacheLength) {
      cache(len)
      System.arraycopy(cacheBuffer, bufferPosition, buf, offset, len)
      update(len)
    } else {
      // bypass caching
      if (bufferStart + bufferPosition + len > end) crap(len)
      input.synchronized {
        input.seek(bufferStart + bufferPosition)
        input.readFully(buf, offset, len)
      }
      invalidateOver(len)
    }
  }

  def readUTF: String = {
    var newPosition = 0L
    var s = ""
    input.synchronized {
      input.seek(bufferStart + bufferPosition)
      s = input.readUTF
      newPosition = input.getFilePointer
    }
    // now update stuff to reflect how much was read
    if (newPosition > bufferStart + bufferLength) invalidateTo(newPosition)
    else bufferPosition = (newPosition - bufferStart).toInt
    s
  }

  def readUnsignedShort: Int = {
    cache(2)
    val a = cacheByte(0)
    val b = cacheByte(1)
    update(2)
    ((a << 8) | (b & 0xff)) & 0xffff
  }

  def readUnsignedByte: Int = readByte & 0xff
  def readBoolean: Boolean = (readByte != 0)
  def readChar: Char = readShort.toChar
  def readDouble: Double = readLong.toDouble
  def readFloat: Float = readInt.toFloat
  def readLong: Long = (readInt.toLong << 32) | (readInt)
  def skipBytes(n: Int): Int = { update(n); n }

  def readByte: Byte = {
    cache(1)
    val result = cacheByte(0)
    update(1)
    result
  }

  def readShort: Short = {
    cache(2)
    val a = cacheByte(0)
    val b = cacheByte(1)
    update(2)
    (a << 8 | b & 0xff).toShort
  }

  def readInt: Int = {
    cache(4)
    val a = cacheByte(0)
    val b = cacheByte(1)
    val c = cacheByte(2)
    val d = cacheByte(3)
    update(4)
    (a & 0xff << 24) | (b & 0xff << 16) | (c & 0xff << 8) | d & 0xff
  }

  @deprecated def readLine: String = ??? // Yep, done intentionally

  // Private methods
  private def seekAbsolute(offset: Long) {
    assert(bufferStart + bufferPosition <= offset,
      s"Cannot seek backwards. at=${bufferStart+bufferPosition}, seek=$offset")

    if (offset - bufferStart < bufferLength)
      // Already cached, just move the ptr forward
      bufferPosition = (offset - bufferStart).toInt
    else
      // invalidate the cache
      invalidateTo(offset)
  }

  // Anything we request from this method has been guaranteed
  // to be <= maxCacheLength. Anything bigger is handled directly
  // and updates the buffer variables accordingly
  private def cache(requested: Int) {
    // if true, we already have it cached
    if (bufferLength - bufferPosition >= requested) return
      // If this happens - dump as much as possible - it's bad.
    if (bufferStart + bufferPosition + requested > end) crap(requested)

    val current = bufferStart + bufferPosition
    bufferLength = math.min((end - current).toInt, maxCacheLength)
    input.synchronized {
      input.seek(current)
      input.readFully(cacheBuffer, 0, bufferLength)
    }
    bufferStart = current
    bufferPosition = 0
  }

  // invalidate to a specific absolute position
  private def invalidateTo(offset: Long) {
    // invalidate the cache - get ready to read in starting @ offset
    bufferStart = offset - bufferLength
    bufferPosition = bufferLength
  }

  // Hopefully these are inlined
  // This is "invalidate this many more bytes over from the current pos"
  @inline private def invalidateOver(dist: Int) =
    invalidateTo(bufferStart + bufferPosition + dist)
  @inline private def update(l: Int) = bufferPosition += l
  @inline private def cacheByte(i: Int): Byte = cacheBuffer(bufferPosition + i)

  // Sent here to die
  private def crap(requested: Int) {
    throw new EOFException(s"""Tried to read past a legal position
in an allocated buffer. Buffer($start, $end), cacheAt=$bufferStart,
rel. pos=$bufferPosition, requested=$requested bytes.
RAF=${input.toString}
""")
  }
}
