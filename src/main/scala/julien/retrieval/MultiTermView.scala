package julien
package retrieval

import gnu.trove.map.hash.TIntIntHashMap
import galago.core.util.ExtentArray

abstract class MultiTermView(terms: Seq[PositionStatsView])
  extends PositionStatsView
  with Bounded
  with NeedsPreparing {

  def children: Seq[Operator] = terms
//  private val loc = {
//    val movers = terms.filter(_.isInstanceOf[Movable])
//    if (movers.isEmpty) null else movers.head.asInstanceOf[Movable]
//  }
  def count: Int = {
//    if (loc != null && countCache.containsKey(loc.at)) {
//      countCache.get(loc.at)
//    } else {
      this.positions.length
 //   }
  }

  // For use in subclasses to hold hits
  protected val hits =  new ExtentArray(10000)

  override lazy val isDense: Boolean = {
    val ops =
      terms.filter(_.isInstanceOf[Operator]).map(_.asInstanceOf[Operator])
    val movers = ops.flatMap(_.movers)
    movers.exists(_.isDense)
  }

  override def size: Int = statistics.docFreq.toInt

  // Start with no knowledge
  val statistics = CountStatistics()

  // Cache the counts for later
  val countCache = new TIntIntHashMap()

  // update the statistics object w/ our notion of "collection length"
  // We *could* say it's dependent on the size of the gram and width, but
  // that's a lot of work and no one else does it, so here's our lazy way out.
  // Override in subclasses with a better value.
  val adjustment = 0

  def updateStatistics(docid: InternalId) = {
    val c = count
    // Don't cache if we can't locate later.
  //  if (loc != null) countCache.put(docid, c)
    statistics.collFreq += c
    statistics.docFreq += 1
    statistics.max = scala.math.min(statistics.max, c)
    statistics.numDocs = terms.head.statistics.numDocs
    statistics.collLength = terms.head.statistics.collLength - adjustment
  }
}
