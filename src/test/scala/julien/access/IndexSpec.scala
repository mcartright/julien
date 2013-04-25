package julien
package access

import org.scalatest._
import org.scalamock.scalatest.MockFactory
import org.lemurproject.galago.core.{index => lemur}
import lemur.{Index => GIndex}
import lemur.LengthsReader.LengthsIterator
import lemur.AggregateReader.CollectionStatistics
import lemur.AggregateReader.IndexPartStatistics

/** A BDD-style testing spec for the [[Index]] class.
  * Most of the one-shot methods are sanity checks. The
  * operational testing of the sequence classes are done
  * in their specs.
  */
class IndexSpec extends FlatSpec with MockFactory with GivenWhenThen {
  def fixture = new {
    // Set up our mock objects
    val mockLengths = mock[LengthsIterator]
    val gidx = mock[GIndex]

    // Some simple statistics
    val fakeCollStats = new CollectionStatistics()
    fakeCollStats.collectionLength = 11473355235L
    fakeCollStats.documentCount = 25342101
    fakeCollStats.fieldName = "document"
    fakeCollStats.maxLength = 412
    fakeCollStats.minLength = 173
    fakeCollStats.avgLength =
      fakeCollStats.collectionLength.toDouble / fakeCollStats.documentCount

    val fakePartStats = new IndexPartStatistics()
    fakePartStats.collectionLength = 11473355235L
    fakePartStats.highestDocumentCount = 25342101
    fakePartStats.highestFrequency = 347
    fakePartStats.partName = "postings"
    fakePartStats.vocabCount = 51354292

    // Expectations during initialization
    (gidx.getLengthsIterator _).expects().returning(mockLengths)
      (gidx.getCollectionStatistics _).
      expects("document").
      returning(fakeCollStats)
      (gidx.getIndexPartStatistics _).
      expects("postings").
      returning(fakePartStats)
  }

  "The test index" should "report correct statistics" in {
    val f = fixture
    import f._

    val testingIndex = Index(gidx)
    expectResult(fakeCollStats.collectionLength) { testingIndex.collectionLength }
    expectResult(fakeCollStats.documentCount) { testingIndex.numDocuments }
    expectResult(fakePartStats.vocabCount) { testingIndex.vocabularySize }
  }

  it should "provide the lengths of documents by docid" in {
    val f = fixture
    import f._

    // The "doc lengths"
    val lmap = Map[Int, Int](1 -> 45, 13 -> 122)
    for ((k,v) <- lmap) (gidx.getLength _).expects(k).returning(v)

    // Our call sequence
    val testingIndex = Index(gidx)
    for ((k,v) <- lmap) {
      expect(new Length(v)) { testingIndex.length(k) }
    }
  }

  it should "return 0 for documents not in the collection" in {
    val f = fixture
    import f._

    (gidx.getLength _).expects(1).returning(0)
    val testingIndex = Index(gidx)
    expect(new Length(0)) { testingIndex.length(1) }
  }

  it should "be able to access a document length by name" in (pending)
  it should "return the positions array of a key/document pair" in (pending)
  it should "return the count of key occuring in a doc" in (pending)
  it should "return an iterator given a valid key" in (pending)
  it should "return a cached iterator given a previously seen key" in (pending)
  it should "return a list of docs given a valid list of docids" in (pending)
  it should "return the name of a document given the docid" in (pending)
  it should "return the docid of a document given the name" in (pending)
  it should "return the term vector of a document given the name" in (pending)
  it should "return the collectdion count of a key" in (pending)
  it should "return the number of documents a key occurs in" in (pending)
}

