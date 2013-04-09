package julien

import java.io.PrintStream
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.tupleflow.Parameters;

object DumpKeys extends CLIFunction {
  def name = "dumpkeys"
  def help = """${name} --index=<index directory>
  Dumps keys from an index file.
  Output is in CSV format.
"""

  def run(p: Parameters, out: PrintStream) : Unit = {
    if (!p.containsKey("index")) {
      out.println(help)
      return
    }

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
