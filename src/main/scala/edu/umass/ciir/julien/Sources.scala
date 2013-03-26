package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.disk.DiskIndex
import org.lemurproject.galago.tupleflow.Utility
import java.util.Properties
import java.io.{File,FileReader}
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

object Sources {
  private[this] val s = {
    val map = HashMap[Symbol, String]()
    val allProps = new Properties
    allProps.load(new FileReader(new File(System.getenv("HOME"), ".julien")))
    val sources = allProps.filter { case (k,v) => k.startsWith("sources") }
    for ((k,v) <- sources) {
      val sym = Symbol(k.split("\\.")(1))
      Console.printf("Adding %s -> %s\n", sym, v)
      map.update(sym, v) }
    map
  }

  private[this] val indexes = new HashMap[Symbol, DiskIndex]()

  def get(symbol: Symbol) : DiskIndex = indexes.get(symbol) match {
    case Some(index) => index
    case None => {
      indexes(symbol) = new DiskIndex(s(symbol))
      indexes(symbol)
    }
  }
}

object Classifiers {
  def muc7 =
    "/usr/ayr/tmp1/irmarc/projects/julien/localdata/english.muc.7class.distsim.crf.ser.gz"

}

object Stopwords {
  def inquery =
    Utility.readStreamToStringSet(
      classOf[DiskIndex].getResourceAsStream("/stopwords/inquery")
    )
}
