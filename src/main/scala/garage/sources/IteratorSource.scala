package garage
package sources

import org.lemurproject.galago.core.util.{ExtentArray,ExtentArrayIterator}
import org.lemurproject.galago.tupleflow.Parameters
import org.lemurproject.galago.core.index._
import julien._

object IteratorSource {
  def gatherStatistics(e: ExtentIterator) : NS = {
    val stats = new NS
    stats.nodeDocumentCount = 0
    stats.nodeFrequency = 0
    stats.maximumCount = 0
    while (!e.isDone) {
      if (e.hasMatch(e.currentCandidate())) {
        stats.nodeFrequency += e.count()
        stats.maximumCount = scala.math.max(e.count(), stats.maximumCount)
        stats.nodeDocumentCount += 1
      }
      e.movePast(e.currentCandidate())
    }
    stats
  }
}

class IteratorSource(val key: String, index: GIndex)
    extends KeyedSource with Stored {
  private val iterator: ExtentIterator =
    index.getIterator(key, Parameters.empty).asInstanceOf[ExtentIterator]
  private val stats: NS = iterator match {
    case n: NullExtentIterator => AggregateReader.NodeStatistics.zero
    case a: ARNA => a.getStatistics
    case e: ExtentIterator => IteratorSource.gatherStatistics(e)
  }

  def count(targetId: String): Int = positions(targetId).size

  def positions(targetId: String): ExtentArray = {
    iterator.reset
    val docid = index.getIdentifier(targetId)
    iterator.syncTo(docid)
    if (iterator.hasMatch(docid)) iterator.extents else ExtentArray.empty
  }

  def collectionCount: Long = stats.nodeFrequency
  def docFreq: Long = stats.nodeDocumentCount

  def supports: Set[Stored] = Set[Stored](this)
}
