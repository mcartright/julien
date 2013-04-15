package julien

import java.util.Properties
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import java.io.{File,FileReader}

object JulienProperties {

  var conf : Properties = null
  val defaultPropertiesFile = "./config/sources.julien"
  loadProperties(defaultPropertiesFile)

  def sources = s

  def loadProperties(propertiesFile : String) = {
     println(s"try loading julien loading properties: $propertiesFile")
     try {
       val properties = System.getProperties()
       val propStream = new FileReader(new File(propertiesFile))
       properties.load(propStream)
       println(s"...loaded from $propertiesFile")
       conf = properties
     } catch {
       case e: Throwable =>
         println(s"Unable to load file : $propertiesFile ${e.getMessage}")
       throw e
     }
  }

  private[this] val s = {
    val map = HashMap[Symbol, String]()
    val sources = conf.filter { case (k,v) => k.startsWith("julien.sources") }
    for ((k,v) <- sources) {
      val sym = Symbol(k.split("\\.")(2))
      Console.printf("Adding %s -> %s\n", sym, v)
      map.update(sym, v) }
    map
  }

  def printProperties() {
    println("JulienProperties:")

    val julienProperties =
      conf.keys.toList.filterNot(e => (e.toString.startsWith("julien")))
    for ( key <- conf.keys) {
      if (key.toString.startsWith("julien.")) {
          println(key + ":" + conf.getProperty(key.toString))
      }
    }
  }
}
