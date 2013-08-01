package julien
package cli
package examples

import julien._
import access.Index
import retrieval._
import retrieval.Utils._
import retrieval.processor._

/**
 * User: jdalton
 * Date: 4/24/13
 */
object EntityDocumentTestMain extends App {

  // load index into memory
  val useMem = true
  implicit val index: Index =
    if (useMem)
    Index.memory("/usr/dan/users4/jdalton/code/julien/src/test/resources/wiki-trectext-5.dat")
  else
    Index.disk("/usr/dan/users4/jdalton/code/julien/data/test-index")

  val lengths = index.underlying.getIndexPart("lengths")
  val keys = lengths.keys()
  while (!keys.isDone) {
    keys.nextKey()
    println(s"${keys.getKey} ${keys.getKeyString}")
  }
  println(s"docs: ${index.numDocuments()} cf: ${index.collectionLength()}")

  val query = args(0).split(" ")
  val seqdep = sdm(query, Dirichlet.apply _)

  // run it and get results
  val results = QueryProcessor(seqdep)
  printResults(results, index)
}
