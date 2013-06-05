package julien
package retrieval

import gnu.trove.map.hash.TIntIntHashMap
import galago.core.util.ExtentArray
import scala.annotation.tailrec
import julien.behavior._

abstract class MultiTermView(terms: Seq[PositionStatsView])
    extends PositionStatsView
    with Conjunction
    with Finite
    with NeedsPreparing {

  lazy val movables: Seq[Movable] = terms.
    filter(_.isInstanceOf[Movable]).
    map(_.asInstanceOf[Movable])

  def children: Seq[Operator] = terms
  def count(id: InternalId): Int =
    //if (countCache.containsKey(id)) countCache.get(id)
    //else
    this.positions(id).length

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
    val c = count(docid)
    // countCache.put(docid, c)
    statistics.collFreq += c
    if (c > 0) statistics.docFreq += 1
    statistics.max = scala.math.max(statistics.max, c)
    statistics.numDocs = terms.head.statistics.numDocs
    statistics.collLength = terms.head.statistics.collLength - adjustment
  }

  @tailrec
  final def ensurePosition(id: InternalId, idx: Int = 0): Boolean =
    if (idx >= movables.length) return true
    else {
      if (!movables(idx).moveTo(id)) return false
      else ensurePosition(id, idx+1)
    }

  case class Posting(var docid: Int, var positions: ExtentArray)
}
