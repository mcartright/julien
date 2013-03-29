package edu.umass.ciir.julien

import org.lemurproject.galago.core.index.disk.DiskIndex
import org.lemurproject.galago.core.index._


object IndexSource {
  type ARCA = AggregateReader.CollectionAggregateIterator
  type ARNA = AggregateReader.NodeAggregateIterator
  type TEI = ExtentIterator
  type TCI = CountIterator
  type MLI = LengthsReader.LengthsIterator

}

class IndexSource(index: DiskIndex) extends FreeSource {
  def this(s: String) = this(new DiskIndex(s))

  private val lengthsIterator = index.getLengthsIterator
  private val collectionStats =
    lengthsIterator.asInstanceOf[ARCA].getStatistics
  private val postingsStats =  index.getIndexPartStatistics("postings")

  def collectionLength: Long = collectionStats.collectionLength
  def numDocuments: Long = collectionStats.documentCount
  def vocabularySize: Long = postingsStats.vocabCount
}
