package julien
package access

import org.lemurproject.galago.core.index.disk.DiskIndex
import org.lemurproject.galago.core.index.corpus.CorpusReader
import org.lemurproject.galago.core.index.mem.MemoryIndex
import org.lemurproject.galago.core.index.{Index=> _, _}
import org.lemurproject.galago.core.index.AggregateReader._
import org.lemurproject.galago.core.util.ExtentArray
import org.lemurproject.galago.core.parse.{Document => _, _}
import org.lemurproject.galago.tupleflow.{Parameters,Utility,Source}
import scala.collection.JavaConversions._
import julien._

object Index {
  private val dummyBytes = Utility.fromString("dummy")
  def apply(i: DiskIndex) = new Index("unknown", i)
  def apply(m: MemoryIndex) = new Index("unknown", m)
  def apply(i: GIndex) = new Index("unknown", i)
  def disk(s: String) = new Index(s, new DiskIndex(s))
  def memory(s: String*) : Index = {
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
    return new Index(s.mkString(","), memoryIndex)
  }
}

class Index(label: String, val underlying: GIndex) {
  override def toString: String = {
    val b = new StringBuilder()
    val hdr = if (underlying.isInstanceOf[MemoryIndex])
      b ++= "memory:"
    else if (underlying.isInstanceOf[DiskIndex])
      b ++= "disk:"
    else
      b ++= "interface:"
    b ++= label
    b.result
  }

  private val lengthsIterator = underlying.getLengthsIterator
  private val collectionStats =
    underlying.getCollectionStatistics("document")
  private val postingsStats =
    underlying.getIndexPartStatistics("postings")

  def lengthsIterator(field: String = "document"): LI =   {
    // remove this after merge field in old galago the key needed to be "document" for lengths.
   // val realField = "document"
    underlying.getIndexPart("lengths").getIterator(field).asInstanceOf[LI]
  }

  def collectionLength: Long = collectionStats.collectionLength
  def numDocuments: Long = collectionStats.documentCount
  def vocabularySize: Long = postingsStats.vocabCount

  def length(d: Docid): Length = Length(underlying.getLength(d))
  def length(targetId: String): Length =
    underlying.getLength(underlying.getIdentifier(targetId))

  def positions(key: String, targetId: String): Positions = {
    val it =
      underlying.getIterator(key, Parameters.empty).asInstanceOf[ExtentIterator]
    if (it.isInstanceOf[NullExtentIterator]) return Positions.empty
    val docid = underlying.getIdentifier(targetId)
    it.syncTo(docid)
    if (it.hasMatch(docid)) Positions(it.extents) else Positions.empty
  }

  def documentIterator(): DataIterator[GDoc] =
    underlying.
      getIndexPart("corpus").
      asInstanceOf[CorpusReader].
      getIterator(Index.dummyBytes).
      asInstanceOf[DataIterator[GDoc]]


  /** Provided because the underlying interface provides it. However it's a
    * breach in the abstraction, and should go away in the future.
    */
  @deprecated
  def partReader(name: String): IndexPartReader = underlying.getIndexPart(name)
  private val iteratorCache =
    scala.collection.mutable.HashMap[String, ExtentIterator]()

  /** Produces a cached ExtentIterator if possible. If not found, a new iterator
    * is constructed and cached for later.
    */
  def shareableIterator(
    key: String,
    field: String = "postings"): ExtentIterator = {
    if (!iteratorCache.contains(key)) {
      iteratorCache(key) = iterator(key, field)
    }
    iteratorCache(key)
  }

  /** Returns an ExtentIterator from the underlying index. If the requested
    * index part is missing, an assertion fails. If the key is missing, a
    * NullExtentIterator is returned.
    */
  def iterator(key: String, field: String = "postings"): ExtentIterator = {
    val part = underlying.getIndexPart(getLabel(field))
    val iter = part.getIterator(key)
    if (iter != null) iter.asInstanceOf[ExtentIterator]
    else new NullExtentIterator(label)
  }

  def postings(key:String): PostingSeq[PositionsPosting] =
    new PostingSeq(iterator(key), this)
  def documents: DocumentSeq = DocumentSeq(this)
  def documents(docids: List[Docid]): List[Document] = {
    val sortedNames = docids.sorted.map(underlying.getName(_))
    val gdocs: scala.collection.mutable.Map[String, GDoc] =
      underlying.getItems(sortedNames, Parameters.empty)
    val jdocs = gdocs.values.map(DocumentClone(_)).toList
    jdocs.sortBy(_.identifier)
  }

  @inline private def getLabel(f: String): String = {
    val label = if (f.startsWith("postings")) f else s"field.$f"
    assume(underlying.containsPart(label), s"Part $label is not in this index")
    label
  }

  /** Returns a view of the set of keys of a given index part.
    * An assertion fails if the part is not found.
   */
  def vocabulary(field: String = "document"): KeySet = {
    new KeySet(underlying.getIndexPart(getLabel(field)).keys _)
  }
  def name(docid: Docid) : String = underlying.getName(docid)
  def identifier(name: String): Docid =
    new Docid(underlying.getIdentifier(name))
  def names: PairSeq[String] =
    new PairSeq[String](underlying.getIndexPart("names").keys,
    (k: KeyIterator) => Utility.toString(k.getValueBytes) : String )
  def count(key: String, targetId: String): Int = positions(key, targetId).size
  def collectionCount(key: String): Long = getKeyStatistics(key).nodeFrequency
  def docFreq(key: String): Long = getKeyStatistics(key).nodeDocumentCount
  def document(docid: Docid): Document =
    IndexBasedDocument(underlying.getItem(underlying.getName(docid.underlying),
      Parameters.empty), this)
  def document(targetId: String): Document =
    IndexBasedDocument(underlying.getItem(targetId, Parameters.empty), this)

  def terms(targetId: String): List[String] = {
    val doc = underlying.getItem(targetId, Parameters.empty)
    doc.terms.toList
  }

  private def getKeyStatistics(key: String) : NS = {
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
