package julien

/** High-level container of the "retrieval" behaviors in Julien.
  * This package contains all the definitions for operators (views
  * and features), as well as the processor classes necessary to
  * execute a query.
  *
  * This package makes substantial use of the functionality provided
  * by the access API.
  */

import scala.reflect.runtime.universe._

package object retrieval {
  type Combiner = (Seq[FeatureOp]) => Double

  // Bring in local references to some of the access structures
  type Index = julien.access.Index
  val Index = julien.access.Index

  import julien.{galago => G}
  // Stopwords here - somewhere else?
  object Stopwords {
    def inquery =
      G.tupleflow.Utility.readStreamToStringSet(
        classOf[G.core.index.Index].getResourceAsStream("/stopwords/inquery")
      )
  }

  def getType[T](x: T)(implicit tag: TypeTag[T]) = tag.tpe
}
