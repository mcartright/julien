package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.disk.DiskIndex
import org.lemurproject.galago.core.index.mem.MemoryIndex
import org.lemurproject.galago.core.index._
import org.lemurproject.galago.core.index.AggregateReader._
import org.lemurproject.galago.core.util.ExtentArray
import org.lemurproject.galago.core.parse._
import org.lemurproject.galago.tupleflow.{Parameters,Utility,Source}
import scala.collection.JavaConversions._

object Index {
  def apply(i: DiskIndex) = new Index(i)
  def apply(m: MemoryIndex) = new Index(m)
  def disk(s: String) = new Index(new DiskIndex(s))
  def memory(s: String*) : Index = {
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
    return new Index(memoryIndex)
  }
}

class Index(val underlying: org.lemurproject.galago.core.index.Index) {
  import edu.umass.ciir.julien.Aliases._

  val lengthsIterator = underlying.getLengthsIterator
  private val collectionStats =
    lengthsIterator.asInstanceOf[ARCA].getStatistics
  private val postingsStats =
    underlying.getIndexPartStatistics("postings")

  def collectionLength: Long = collectionStats.collectionLength
  def numDocuments: Long = collectionStats.documentCount
  def vocabularySize: Long = postingsStats.vocabCount

  def length(targetId: String): Int =
    underlying.getLength(underlying.getIdentifier(targetId))

  def positions(key: String, targetId: String): ExtentArray = {
    val it =
      underlying.getIterator(key, Parameters.empty).asInstanceOf[ExtentIterator]
    if (it.isInstanceOf[NullExtentIterator]) return ExtentArray.empty
    val docid = underlying.getIdentifier(targetId)
    it.syncTo(docid)
    if (it.hasMatch(docid)) it.extents else ExtentArray.empty
  }

  def iterator(key: String): ExtentIterator =
    underlying.getIterator(key, Parameters.empty).asInstanceOf[ExtentIterator]

  def postings(key:String): PostingSeq[PositionsPosting] =
    new PostingSeq(iterator(key), this)
  def documents: DocumentSeq[Document] =
    new DocumentSeq[IndexBasedDocument](this)
  def vocabulary: KeySeq = new KeySeq(underlying.getIndexPart("postings").keys)


  def count(key: String, targetId: String): Int = positions(key, targetId).size
  def collectionCount(key: String): Long = getKeyedStatistics(key).nodeFrequency
  def docFreq(key: String): Long = getKeyedStatistics(key).nodeDocumentCount
  def document(targetId: String): org.lemurproject.galago.core.parse.Document =
    underlying.getItem(targetId, Parameters.empty)

  def terms(targetId: String): List[String] = {
    val doc = underlying.getItem(targetId, Parameters.empty)
    doc.terms.toList
  }

  private def getKeyedStatistics(key: String) : NS = {
    val it = underlying.getIterator(key, Parameters.empty)
    it match {
      case n: NullExtentIterator => AggregateReader.NodeStatistics.zero
      case a: ARNA => a.getStatistics
      case e: ExtentIterator => gatherStatistics(e)
    }
  }

  private def gatherStatistics(e: ExtentIterator): NS = {
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
