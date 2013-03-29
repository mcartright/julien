package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.disk.DiskIndex
import org.lemurproject.galago.core.index.mem.MemoryIndex
import org.lemurproject.galago.core.index._
import org.lemurproject.galago.core.index.AggregateReader._
import org.lemurproject.galago.core.parse._
import org.lemurproject.galago.tupleflow.{Parameters,Utility,Source}
import scala.collection.JavaConversions._


object IndexSource {
  def apply(i: DiskIndex) = new IndexSource(i)
  def apply(m: MemoryIndex) = new IndexSource(m)
  def disk(s: String) = new IndexSource(new DiskIndex(s))
  def memory(s: String*) : IndexSource = {
    // Try to use the components from the Galago pipeline to
    // 1) Chop the file into a DocumentSource
    val docsource = new DocumentSource(s: _*)

    // Establish the pipeline
    val memoryIndex = docsource.asInstanceOf[Source[_]].
      setProcessor(new ParserSelector()).asInstanceOf[Source[_]].
      setProcessor(new TagTokenizer()).asInstanceOf[Source[_]].
      setProcessor(new DocumentNumberer()).asInstanceOf[Source[_]].
      setProcessor(new MemoryIndex()).asInstanceOf[MemoryIndex]

    // Run it
    docsource.run()
    // Return it
    return new IndexSource(memoryIndex)
  }
}

class IndexSource(index: Index) extends FreeSource with Stored {
  type ARCA = AggregateReader.CollectionAggregateIterator
  type ARNA = AggregateReader.NodeAggregateIterator
  type NS = AggregateReader.NodeStatistics
  type TEI = ExtentIterator
  type TCI = CountIterator
  type MLI = LengthsReader.LengthsIterator
  implicit def string2bytes(s: String) = Utility.fromString(s)

  private val lengthsIterator = index.getLengthsIterator
  private val collectionStats =
    lengthsIterator.asInstanceOf[ARCA].getStatistics
  private val postingsStats =  index.getIndexPartStatistics("postings")

  def supports: Set[Stored] = Set[Stored](this)

  def collectionLength: Long = collectionStats.collectionLength
  def numDocuments: Long = collectionStats.documentCount
  def vocabularySize: Long = postingsStats.vocabCount

  def length(targetId: String): Int =
    index.getLength(index.getIdentifier(targetId))

  def count(key: String, targetId: String): Int = {
    val it =
      index.getIterator(key, Parameters.empty).asInstanceOf[CountIterator]
    if (it.isInstanceOf[NullExtentIterator]) return 0
    val docid = index.getIdentifier(targetId)
    it.syncTo(docid)
    if (it.hasMatch(docid)) it.count else 0
  }

  def collectionCount(key: String): Long = getKeyedStatistics(key).nodeFrequency

  def docCount(key: String): Long = getKeyedStatistics(key).nodeDocumentCount

  def document(targetId: String): Document =
    index.getItem(targetId, Parameters.empty)

  def terms(targetId: String): List[String] = {
    val doc = index.getItem(targetId, Parameters.empty)
    doc.terms.toList
  }

  private def getKeyedStatistics(key: String) : NS = {
    val it = index.getIterator(key, Parameters.empty)
    it match {
      case n: NullExtentIterator => new AggregateReader.NodeStatistics
      case a: ARNA => a.getStatistics
      case e: ExtentIterator => {
        val stats = new NodeStatistics
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
  }
}
