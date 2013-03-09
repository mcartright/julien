import org.lemurproject.galago.core.index.disk.DiskIndex
import org.lemurproject.galago.core.retrieval.iterator.MovableLengthsIterator
import org.lemurproject.galago.core.retrieval.query.{StructuredQuery, Node}
import org.lemurproject.galago.core.index.disk.PositionIndexReader
import scala.collection.mutable.PriorityQueue

import scala.collection.mutable.LinkedHashMap

// Hack for now to load some common definitions
import GalagoBridging._

// Query prep
val query = "new york city"
val queryNodes = bowNodes(query)

val aquaint = Sources.aquaint
val lengths = aquaint.getLengthsIterator
val nodeMap = LinkedHashMap[Node, java.lang.Object]()
for (n <- queryNodes) { nodeMap.update(n,
  aquaint.getIterator(n).asInstanceOf[PositionIndexReader#TermCountIterator])
}

val iterators = nodeMap.values.toList.map(obj2movableIt(_))

// Create the scoring functions that map over the iterators
val scorers = unigrams(nodeMap, aquaint, lengths)

// Scoring loop
val resultQueue = standardScoringLoop(scorers, iterators, lengths)

// Get doc names and print
printResults(resultQueue, aquaint)
