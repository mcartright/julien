package julien
package access

import org.scalatest.FlatSpec
import org.scalamock.scalatest.MockFactory
import org.lemurproject.galago.core.{index => lemur}
import lemur.{Index => GIndex}
import lemur.LengthsReader.LengthsIterator
import lemur.AggregateReader.CollectionStatistics
import lemur.AggregateReader.IndexPartStatistics

class IndexSpec extends FlatSpec with MockFactory {
  // Make a shared fixture for all the tests
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


    (gidx.getLengthsIterator _).expects().returning(mockLengths)
    (gidx.getCollectionStatistics _).
      expects("postings").
      returning(fakeCollStats)
    (gidx.getIndexPartStatistics _).
      expects("postings").
      returning(fakePartStats)
  }

  "The test index" should "report correct statistics" in {
    val f = fixture
    import f._

    val testingIndex = Index(gidx)
    expect(fakeCollStats.collectionLength) { testingIndex.collectionLength }
    expect(fakeCollStats.documentCount) { testingIndex.numDocuments }
    expect(fakePartStats.vocabCount) { testingIndex.vocabularySize }
  }
}

