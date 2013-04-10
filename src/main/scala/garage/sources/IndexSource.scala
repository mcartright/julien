package garage
package sources

import org.lemurproject.galago.core.index._
import org.lemurproject.galago.core.index.AggregateReader._
import org.lemurproject.galago.core.util.ExtentArray
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
      setProcessor(new ParserCounter()).asInstanceOf[Source[_]].
      setProcessor(new SplitOffsetter()).asInstanceOf[Source[_]].
      setProcessor(new ParserSelector()).asInstanceOf[Source[_]].
      setProcessor(new TagTokenizer()).asInstanceOf[Source[_]].
      setProcessor(new MemoryIndex()).asInstanceOf[MemoryIndex]

    // Run it
    docsource.run()
    // Return it
    return new IndexSource(memoryIndex)
  }
}

class IndexSource(index: Index) extends FreeSource with Stored {
  import julien._

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

  def positions(key: String, targetId: String): ExtentArray = {
    val it =
      index.getIterator(key, Parameters.empty).asInstanceOf[ExtentIterator]
    if (it.isInstanceOf[NullExtentIterator]) return ExtentArray.empty
    val docid = index.getIdentifier(targetId)
    it.syncTo(docid)
    if (it.hasMatch(docid)) it.extents else ExtentArray.empty
  }

  def count(key: String, targetId: String): Int = positions(key, targetId).size
  def collectionCount(key: String): Long = getKeyedStatistics(key).nodeFrequency
  def docFreq(key: String): Long = getKeyedStatistics(key).nodeDocumentCount
  def document(targetId: String): GDoc =
    index.getItem(targetId, Parameters.empty)

  def terms(targetId: String): List[String] = {
    val doc = index.getItem(targetId, Parameters.empty)
    doc.terms.toList
  }

  private def getKeyedStatistics(key: String) : NS = {
    val is = new IteratorSource(key, index)

    val it = index.getIterator(key, Parameters.empty)
    it match {
      case n: NullExtentIterator => AggregateReader.NodeStatistics.zero
      case a: ARNA => a.getStatistics
      case e: ExtentIterator => IteratorSource.gatherStatistics(e)
    }
  }
}
