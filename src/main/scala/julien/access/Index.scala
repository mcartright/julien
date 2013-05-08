package julien
package access

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.util.logging.{Level,Logger}
import julien.cli.BuildIndex
import julien.galago.core.index.disk.DiskIndex
import julien.galago.core.index.dynamic.DynamicIndex
import julien.galago.core.index.corpus.CorpusReader
import julien.galago.core.index.MemoryIndex
import julien.galago.core.index.Index.IndexPartReader
import julien.galago.core.index.{Index=> _, _}
import julien.galago.core.index.AggregateReader._
import julien.galago.core.util.ExtentArray
import julien.galago.core.parse.{Document => _, _}
import julien.galago.tupleflow.{Parameters,Utility,Source}
import julien.galago.tupleflow.execution.JobExecutor
import scala.collection.JavaConversions._
import julien._

object Index {
  private val dummyBytes = Utility.fromString("dummy")
  def apply(i: DiskIndex):Index = apply(i, "all")
  def apply(i: DiskIndex, defaultPart: String): Index =
    new Index("unknown", i, defaultPart)
  def apply(m: MemoryIndex):Index = apply(m, "all")
  def apply(m: MemoryIndex, defaultPart: String): Index =
    new Index("unknown", m, defaultPart)
  def apply(i: GIndex): Index = apply(i, "all")
  def apply(i: GIndex, defaultPart: String): Index =
    new Index("unknown", i, defaultPart)
  def disk(s: String): Index = disk(s, "all")
  def disk(s: String, defaultPart: String): Index =
    new Index(s, new DiskIndex(s), defaultPart)
  def memory(
    input: String,
    defaultPart: String = "all",
    parameters: Parameters = Parameters.empty): Index = {
    val tmp: File = Utility.createTemporaryDirectory("memory")
    parameters.set("inputPath", List(input))
    parameters.set("indexPath", tmp.getAbsolutePath)
    Logger.getLogger("").setLevel(Level.OFF)
    Logger.getLogger(classOf[JobExecutor].toString).setLevel(Level.OFF)
    val receiver = new PrintStream(new ByteArrayOutputStream)
    BuildIndex.run(parameters, receiver)
    receiver.close

    // Open, stuff into a memory index
    val memoryIndex = new MemoryIndex(tmp.getAbsolutePath)

    // Can delete underlying segments because the memory index eagerly
    // loaded
    Utility.deleteDirectory(tmp)

    // Return using the memory index
    return new Index(input, memoryIndex, defaultPart)
  }

  def memoryFromDisk(inputPath:String,  defaultPart: String = "all") : Index = {
    val memoryIndex = new MemoryIndex(inputPath)
    return new Index(inputPath, memoryIndex, defaultPart)
  }


  def dynamic(
    input: String,
    defaultPart: String = "all",
    parameters: Parameters = Parameters.empty): Index = {
    val parserParams = parameters.get("parser", Parameters.empty)
    val tokenizerParams = parameters.get("tokenizer", Parameters.empty)

    // Try to use the components from the Galago pipeline to
    // 1) Chop the file into a DocumentSource
    val docsource = new DocumentSource(List(input), parameters)

    // Establish the pipeline
    val dynamicIndex = docsource.asInstanceOf[Source[_]].
      setProcessor(new ParserCounter(parserParams)).asInstanceOf[Source[_]].
      setProcessor(new SplitOffsetter()).asInstanceOf[Source[_]].
      setProcessor(new ParserSelector(parserParams)).asInstanceOf[Source[_]].
      setProcessor(new TagTokenizer(tokenizerParams)).asInstanceOf[Source[_]].
      setProcessor(new DynamicIndex()).asInstanceOf[DynamicIndex]

    // Run it
    docsource.run()
    // load it
    return new Index(input, dynamicIndex, defaultPart)
  }
}

class Index private(
  label: String,
  val underlying: GIndex,
  private[this] var currentDefault: String) {
  // Make sure the default isn't a crock
  assume(underlying.containsPart(s"$currentDefault.postings"),
    s"$currentDefault is not a part in this index ($toString)")

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

  /** Returns the current default part */
  def defaultPart: String = currentDefault

  /**
    * Sets the current default part. If the part doesn't
    * exist, fails an assertion.
    */
  def defaultPart_=(newDefault: String): Unit = {
    assume(underlying.containsPart(s"$newDefault.postings"),
      s"$newDefault is not a part in this index ($toString)")
    currentDefault = newDefault
  }

  val collectionStats =
    underlying.getCollectionStatistics(currentDefault)
  private val postingsStats =
    underlying.getIndexPartStatistics(getLabel(currentDefault))

  /** In theory, releases the resources associated with this index. In theory.*/
  def close: Unit = underlying.close
  def collectionLength: Long = collectionStats.collectionLength
  def numDocuments: Long = collectionStats.documentCount
  def vocabularySize: Long = postingsStats.vocabCount

  def length(d: InternalId): Int = underlying.getLength(d)
  def length(targetId: String): Int =
    underlying.getLength(underlying.getIdentifier(targetId))

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

  def lengthsIterator(field: String): LI =
    underlying.getIndexPart("lengths").getIterator(field).asInstanceOf[LI]

  def lengthsIterator(field: Option[String] = None): LI =
    lengthsIterator(field.getOrElse(currentDefault))

  def clearIteratorCache: Unit = iteratorCache.clear
  def shareableIterator(
    key: String,
    field: Option[String] = None): ExtentIterator =
    shareableIterator(key, field.getOrElse(currentDefault))

  /** Produces a cached ExtentIterator if possible. If not found, a new iterator
    * is constructed and cached for later.
    */
  def shareableIterator(key: String, field: String): ExtentIterator = {
    if (!iteratorCache.contains(key)) {
      iteratorCache(key) = iterator(key, field)
    }
    iteratorCache(key)
  }

  /** Used to allow for using a default field. If a None is provided, then
    * the current default field is used. Otherwise it will unwrap the
    * Option and use the provided field.
    */
  def iterator(
    key: String,
    field: Option[String] = None): ExtentIterator =
    iterator(key, field.getOrElse(currentDefault))

  /** Returns an ExtentIterator from the underlying index. If the requested
    * index part is missing, an assertion fails. If the key is missing, a
    * NullExtentIterator is returned.
    */
  def iterator(key: String, field: String): ExtentIterator = {
    val label = getLabel(field)
    val part = underlying.getIndexPart(label)
    val iter = part.getIterator(key)
    if (iter != null) iter.asInstanceOf[ExtentIterator]
    else new NullExtentIterator(label)
  }

  def postings(key:String): PostingSeq[PositionsPosting] =
    new PostingSeq(iterator(key), this)
  def documents: DocumentSeq = DocumentSeq(this)
  def documents(docids: Seq[InternalId]): List[Document] = {
    val sortedNames = docids.sorted.map(underlying.getName(_))
    val gdocs: scala.collection.mutable.Map[String, GDoc] =
      underlying.getItems(sortedNames, Parameters.empty)
    val jdocs = gdocs.values.map(DocumentClone(_)).toList
    jdocs.sortBy(_.identifier)
  }


  def positions(key: String, targetId: String): ExtentArray = {
    val it =
      underlying.getIterator(key, Parameters.empty).asInstanceOf[ExtentIterator]
    if (it.isInstanceOf[NullExtentIterator]) return ExtentArray.empty
    val docid = underlying.getIdentifier(targetId)
    it.syncTo(docid)
    if (it.hasMatch(docid)) it.extents else ExtentArray.empty
  }

  /** Returns a view of the set of keys of a given index part.
    * An assertion fails if the part is not found.
   */
  def vocabulary(field: String = currentDefault): KeySet =
    KeySet(underlying.getIndexPart(getLabel(field)).keys _)
  def name(docid: InternalId) : String = underlying.getName(docid)
  def identifier(name: String): InternalId =
    new InternalId(underlying.getIdentifier(name))
  def names: PairSeq[String] =
    new PairSeq[String](underlying.getIndexPart("names").keys,
    (k: KeyIterator) => Utility.toString(k.getValueBytes) : String )
  def count(key: String, targetId: String): Int =
    positions(key, targetId).position
  def collectionCount(key: String): Long = getKeyStatistics(key).nodeFrequency
  def docFreq(key: String): Long = getKeyStatistics(key).nodeDocumentCount
  def document(docid: InternalId): Document =
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

  private def getLabel(field: String): String = {
    val label = s"$field.postings"
    assume (underlying.containsPart(label),
      s"$label is not a part in this index ($toString)")
    label
  }
}
