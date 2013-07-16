package julien

import java.io.{RandomAccessFile,PrintStream}
import julien.galago.tupleflow.Parameters

object OverwriteManifest extends CLIFunction {
  val name = "overwrite-manifest"
  val help = """overwrite-manifest --indexPath=/path/to/index/file --key=value
  Rewrites internal index manifest data for index file.
  Allows parameters to be changed after index files have been written.

  WARNING: Changing some parameters may make the index file non-readable.
"""

  def checksOut(p: Parameters): Boolean = p.isString("indexPath")

  def run(p: Parameters, out: PrintStream) {
    // first open the index
    val filename = p.getString("indexPath");
    val indexReaderWriter = new RandomAccessFile(filename, "rw");

    val length = indexReaderWriter.length
    val footerOffset = length - Integer.SIZE / 8 - 3 * java.lang.Long.SIZE / 8
    indexReaderWriter.seek(footerOffset)

    // read metadata values:
    val vocabularyOffset = indexReaderWriter.readLong
    val manifestOffset = indexReaderWriter.readLong
    val blockSize = indexReaderWriter.readInt
    val magicNumber = indexReaderWriter.readLong

    indexReaderWriter.seek(manifestOffset)
    val dataSize = (footerOffset - manifestOffset).asInstanceOf[Int]
    var xmlData = Array.ofDim[Byte](dataSize)
    indexReaderWriter.read(xmlData)
    val newParameters = Parameters.parse(xmlData)
    newParameters.copyFrom(p)

    indexReaderWriter.seek(manifestOffset)

    // write the new data back to the file
    xmlData = newParameters.toString().getBytes("UTF-8")
    indexReaderWriter.write(xmlData)
    indexReaderWriter.writeLong(vocabularyOffset)
    indexReaderWriter.writeLong(manifestOffset)
    indexReaderWriter.writeInt(blockSize)
    indexReaderWriter.writeLong(magicNumber)
    indexReaderWriter.close()
  }
}
