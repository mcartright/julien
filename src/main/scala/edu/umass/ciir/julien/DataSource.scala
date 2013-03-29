package edu.umass.ciir.julien

import org.lemurproject.galago.core.util.ExtentArray
import org.lemurproject.galago.core.util.ExtentArrayIterator
import org.lemurproject.galago.core.parse.Document

sealed trait DataSource

trait CollectionSource extends DataSource {
  def collectionLength: Long
  def numDocuments: Long
  def vocabularySize: Long
}

trait FreeSource extends DataSource {
  def count(key: String, targetId: String): Int
  def positions(targetId: String): ExtentArray
  def collectionCount(key: String): Long
  def docFreq(key: String): Long
  def length(targetId: String): Int
  def document(targetId: String): Document
  def terms(targetId: String): List[String]
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

sealed trait BoundSource extends DataSource {
  def count: Int
  def positions: ExtentArray
  def collectionCount: Long
  def docFreq: Long
  def length: Int
  def document: Document
  def terms: List[String]
}

trait FixedPointSource extends BoundSource
trait EnvironmentBoundSource extends BoundSource

trait EnvironmentalSource extends DataSource {
  def query: String
  def cache: Map[Any, Any]
}
