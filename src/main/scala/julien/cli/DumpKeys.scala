package julien

import java.io.{PrintStream,File}
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.tupleflow.Parameters;

object DumpKeys extends CLIFunction {
  def name = "dumpkeys"

  def checksOut(p: Parameters): Boolean =  if (p.containsKey("index")) {
    new File(p.getString("index")).isFile
  } else {
    false
  }

  def help = """
Dumps keys from an index file. Output is in CSV format.
Required parameters:

index      path of index *file* to dump. NOT a directory.
"""

  def run(p: Parameters, out: PrintStream) : Unit = {
    val reader = DiskIndex.openIndexPart(p.getString("index"))
    if (reader.getManifest().get("emptyIndexFile", false)) {
      out.println("Empty Index File.")
      return
    }

    val iterator = reader.keys()
    while (!iterator.isDone()) {
      out.println(iterator.getKeyString())
      iterator.nextKey()
    }
    reader.close()
  }
}
