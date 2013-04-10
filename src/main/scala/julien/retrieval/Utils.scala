package julien
package retrieval

import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConversions._
import julien._
import julien.access._

/** Provides printing methods for lists of [[ScoredDocument]]s.
  * Most likely will be factored out in the future.
  */
object Utils {
  // TODO: Remove this, make the ScoredDocuments into a collection type.

  def printResults(results: List[ScoredDocument], index: Index) : Unit = {
    for ((sd, idx) <- results.zipWithIndex) {
      val name = index.name(sd.docid)
      Console.printf("test %s %f %d julien\n",
        name, sd.score, idx+1)
    }
  }

  def printResults(
    results: PriorityQueue[ScoredDocument],
    index: Index) : Unit = {
    var rank = 1
    while (!results.isEmpty) {
      val doc = results.dequeue
      val name = index.name(doc.docid)
      Console.printf("test %s %f %d julien\n",
        name, doc.score, rank)
      rank += 1
    }
  }
}

