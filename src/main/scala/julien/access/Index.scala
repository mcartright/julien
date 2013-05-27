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
  def apply(i: DiskIndex):Index = apply(i, "all", "raw")
  def apply(i: DiskIndex, defaultField: String, defaultStem: String): Index =
    new Index("unknown", i, defaultField, defaultStem)
  def apply(m: MemoryIndex):Index = apply(m, "all", "raw")
  def apply(m: MemoryIndex, defaultField: String, defaultStem: String): Index =
    new Index("unknown", m, defaultField, defaultStem)
  def apply(i: GIndex): Index = apply(i, "all", "raw")
  def apply(i: GIndex, defaultField: String, defaultStem: String): Index =
    new Index("unknown", i, defaultField, defaultStem)
  def disk(s: String): Index = disk(s, "all", "raw")
  def disk(s: String, defaultField: String, defaultStem: String): Index =
    new Index(s, new DiskIndex(s), defaultField, defaultStem)
  def memory(
    input: String,
    defaultField: String = "all",
    defaultStem: String = "raw",
    parameters: Parameters = Parameters.empty): Index = {
    val tmp: File = Utility.createTemporaryDirectory("memory")
    parameters.set("inputPath", List(input))
    parameters.set("indexPath", tmp.getAbsolutePath)
    Logger.getLogger("").setLevel(Level.OFF)
    val receiver = new PrintStream(new ByteArrayOutputStream)
    BuildIndex.run(parameters, receiver)
    receiver.close

    // Open, stuff into a memory index
    val memoryIndex = new MemoryIndex(tmp.getAbsolutePath)

    // Can delete underlying segments because the memory index eagerly
    // loaded
    Utility.deleteDirectory(tmp)

    // Return using the memory index
    return new Index(input, memoryIndex, defaultField, defaultStem)
  }

  def memoryFromDisk(inputPath:String,
    defaultField: String = "all",
    defaultStem: String = "raw"
  ) : Index = {
    val memoryIndex = new MemoryIndex(inputPath)
    return new Index(inputPath, memoryIndex, defaultField, defaultStem)
  }


  def dynamic(
    input: String,
    defaultField: String = "all",
    defaultStem: String = "raw",
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
    return new Index(input, dynamicIndex, defaultField, defaultStem)
  }
}

class Index private(
  label: String,
  val underlying: GIndex,
  private[this] var impliedField: String,
  private[this] var impliedStem: String) {
  // Make sure the default isn't a crock
  checkConfiguration

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

  /** Returns the current default stemming pattern */
  def defaultStem: String = impliedStem

  /** Sets the current default stemming pattern. */
  def defaultStem_=(newDefault: String) {
    impliedStem = newDefault
    checkConfiguration
  }

  /** Returns the current default part */
  def defaultField: String = impliedField

  /**
    * Sets the current default part. If the part doesn't
    * exist, fails an assertion.
    */
  def defaultField_=(newDefault: String) {
    impliedField = newDefault
    checkConfiguration
  }

  val collectionStats =
    underlying.getCollectionStatistics(impliedField)
  private val postingsStats =
    underlying.getIndexPartStatistics(getLabel())

  /** In theory, releases the resources associated with this index. In theory.*/
  def close: Unit = underlying.close
  def collectionLength: Long = collectionStats.collectionLength
  def numDocuments: Long = collectionStats.documentCount
  def vocabularySize: Long = postingsStats.vocabCount
  def partSize(s: String): Long = underlying.indexPartSize(s)
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
  @deprecated("partReader is a breach of abstraction", "all versions")
  def partReader(name: String): IndexPartReader = underlying.getIndexPart(name)
  private val iteratorCache =
    scala.collection.mutable.HashMap[String, ExtentIterator]()

  def lengthsIterator(field: String): LI =
    underlying.getIndexPart("lengths").getIterator(field).asInstanceOf[LI]

  def lengthsIterator(field: Option[String] = None): LI =
    lengthsIterator(field.getOrElse(impliedField))

  def clearIteratorCache: Unit = iteratorCache.clear

  /** Produces a cached ExtentIterator if possible. If not found, a new iterator
    * is constructed and cached for later.
    */
  def shareableIterator(
    key: String,
    field: String = defaultField,
    stem: String = defaultStem): ExtentIterator = {
    if (!iteratorCache.contains(key)) {
      iteratorCache(key) = iterator(key, field, stem)
    }
    iteratorCache(key)
  }

  /** Returns an ExtentIterator from the underlying index. If the requested
    * index part is missing, an assertion fails. If the key is missing, a
    * NullExtentIterator is returned.
    */
  def iterator(key: String,
    field: String = defaultField,
    stem: String = defaultStem): ExtentIterator = {
    val label = getLabel(field, stem)
    val part = underlying.getIndexPart(label)
    val iter = part.getIterator(key)
    if (iter != null) iter.asInstanceOf[ExtentIterator]
    else new NullExtentIterator(label)
  }

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
  def vocabulary(
    field: String = impliedField,
    stem: String = impliedStem
  ): KeySet = KeySet(underlying.getIndexPart(getLabel(field, stem)).keys _)
  def name(docid: InternalId) : String = underlying.getName(docid)
  def identifier(name: String): InternalId =
    new InternalId(underlying.getIdentifier(name))
  def count(key: String, targetId: String): Int =
    positions(key, targetId).length
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

  private def getLabel(
    field: String = defaultField,
    stem: String = defaultStem): String = {
    val label = s"$field.postings.$stem"
    assume (underlying.containsPart(label),
      s"$label is not a part in this index ($toString)")
    label
  }

  private def checkConfiguration {
    val testPart = s"$impliedField.postings.$impliedStem"
    assume(underlying.containsPart(testPart),
      s"$testPart is not in this index.")
  }
}
