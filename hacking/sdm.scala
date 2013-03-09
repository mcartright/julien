import org.lemurproject.galago.core.index.disk.DiskIndex
import org.lemurproject.galago.core.retrieval.iterator.MovableLengthsIterator
import org.lemurproject.galago.core.retrieval.query.{StructuredQuery, Node}
import org.lemurproject.galago.core.index.disk.PositionIndexReader
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.ListBuffer

import scala.collection.mutable.LinkedHashMap

import GalagoBridging._

// Query prep
val query = "new york city"
val queryNodes = bowNodes(query, "extents")
val aquaint = Sources.aquaint
val lengths = aquaint.getLengthsIterator
val nodeMap = LinkedHashMap[Node, java.lang.Object]()
for (n <- queryNodes) { nodeMap.update(n,
  aquaint.getIterator(n).asInstanceOf[PositionIndexReader#TermExtentIterator])
}

val iterators = nodeMap.values.toList.map(obj2movableIt(_))

// Make the scorers - more complex here because we use unigrams,
// phrases, and windows, and each one is under a combining scorer.
val scorers = List(
  ParameterizedScorer(unigrams(nodeMap, aquaint, lengths), 0.8),
  ParameterizedScorer(orderedWindows(nodeMap, aquaint, lengths), 0.15),
  ParameterizedScorer(unorderedWindows(nodeMap, aquaint, lengths, 8), 0.05)
)

// Scoring loop
val resultQueue = standardScoringLoop(scorers, iterators, lengths)

// Get doc names and print
printResults(resultQueue, aquaint)
