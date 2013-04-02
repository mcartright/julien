package edu.umass.ciir.julien

import org.lemurproject.galago.core.index._
import org.lemurproject.galago.core.index.corpus._
import org.lemurproject.galago.tupleflow.Utility

object Aliases {
  type GIndex = org.lemurproject.galago.core.index.Index
  type GDoc = org.lemurproject.galago.core.parse.Document
  type ARCA = AggregateReader.CollectionAggregateIterator
  type ARNA = AggregateReader.NodeAggregateIterator
  type NS = AggregateReader.NodeStatistics
  type CS = AggregateReader.CollectionStatistics
  type TEI = ExtentIterator
  type TCI = CountIterator
  type MLI = LengthsReader.LengthsIterator
  implicit def string2bytes(s: String) = Utility.fromString(s)

}
