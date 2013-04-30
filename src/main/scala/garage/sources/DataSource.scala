package garage
package sources

import julien.galago.core.util.ExtentArray
import julien.galago.core.util.ExtentArrayIterator
import julien.galago.core.parse.Document

sealed trait DataSource

trait CollectionSource extends DataSource {
  def collectionLength: Long
  def numDocuments: Long
  def vocabularySize: Long
}

trait FreeSource extends DataSource { self: FreeSource =>
  def count(key: String, targetId: String): Int
  def positions(key: String, targetId: String): ExtentArray
  def collectionCount(key: String): Long
  def docFreq(key: String): Long
  def length(targetId: String): Int
  def document(targetId: String): Document
  def terms(targetId: String): List[String]

  def bindKey(key: String): KeyedSource = new KeyedSource {
    def count(targetId: String): Int = self.count(key, targetId)
    def positions(targetId: String): ExtentArray = self.positions(key, targetId)
    def collectionCount: Long = self.collectionCount(key)
    def docFreq: Long = self.docFreq(key)
  }

  def bindTarget(targetId: String): TargetedSource = new TargetedSource {
    def length: Int = self.length(targetId)
    def document: Document = self.document(targetId)
    def terms: List[String] = self.terms(targetId)
  }

  def bindFully(key: String, targetId: String) = new BoundSource {
    def count: Int = self.count(key, targetId)
    def positions: ExtentArray = self.positions(key, targetId)
    def collectionCount: Long = self.collectionCount(key)
    def docFreq: Long = self.docFreq(key)
    def length: Int = self.length(targetId)
    def document: Document = self.document(targetId)
    def terms: List[String] = self.terms(targetId)
  }
}

trait KeyedSource extends DataSource {
  def count(targetId: String): Int
  def positions(targetId: String): ExtentArray
  def collectionCount: Long
  def docFreq: Long
}

trait TargetedSource extends DataSource {
  def length: Int
  def document: Document
  def terms: List[String]
}

trait BoundSource extends DataSource {
  def count: Int
  def positions: ExtentArray
  def collectionCount: Long
  def docFreq: Long
  def length: Int
  def document: Document
  def terms: List[String]
}

trait EnvironmentalSource extends DataSource {
  def currentKey: String
  def currentDocument: String
  def query: String
  def cache: Map[Any, Any]
}
