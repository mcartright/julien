package julien
package retrieval

import julien.retrieval.processor._

object LinePrintingDebugger { def apply() = new LinePrintingDebugger() }
class LinePrintingDebugger {
  def printState(
    sObj: ScoredDocument,
    features: Seq[Feature],
    index: Index,
    processor: QueryProcessor
  ) {
    val b = StringBuilder.newBuilder
    b ++= sObj.toString += ';'
    b ++= features.mkString(",") += ';'
    b ++= index.toString += ';'
    println(Console.MAGENTA + b.toString + Console.RESET)
  }
}
