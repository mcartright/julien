package edu.umass.ciir.julien

import org.lemurproject.galago.core.util.ExtentArray
import org.lemurproject.galago.core.util.ExtentArrayIterator
import org.lemurproject.galago.core.parse.Document

sealed trait DataSource

trait FreeSource extends DataSource {
  def collectionLength: Long
  def numDocuments: Long
  def vocbularySize: Long
  def length(targetId: String): Int
  def count(key: String, targetId: String): Int
  def collectionCount(key: String): Long
  def docCount(key: String): Long
  def docFreq(key: String): Long
  def inverseDocFreq(key: String): Double =
    scala.math.log(docCount(key) / (docFreq(key) + 0.5))
  def document(targetId: String): Document
  def terms(targetId: String): List[String]
}

trait KeyedSource extends FreeSource {
  def count(targetId: String): Int
  def positions(targetId: String): ExtentArray
  def collectionCount: Long
  def docCount: Long
  def docFreq: Long
  def inverseDocFreq: Double =
    scala.math.log(docCount / (docFreq + 0.5))
}

trait TargetedSource extends FreeSource {
  def length: Int
  def document: Document
  def terms: List[String]
}

trait BoundSource extends KeyedSource with TargetedSource {
  def count: Int
  def positions: ExtentArray
}

trait EnvironmentalSource extends DataSource {
  def query: String
  def cache: Map[Any, Any]
}
