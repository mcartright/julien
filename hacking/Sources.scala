import org.lemurproject.galago.core.index.disk.DiskIndex
import org.lemurproject.galago.tupleflow.Utility

object Sources {
  def aquaint =
    new DiskIndex("/usr/ayr/tmp1/irmarc/data/indexes/aquaint-fielded")
  def gov2 =
    new DiskIndex("/usr/ayr/tmp1/irmarc/data/indexes/gov2-fielded")
}

object Stopwords {
  def inquery =
    Utility.readStreamToStringSet(
      classOf[DiskIndex].getResourceAsStream("/stopwords/inquery")
    )
}
