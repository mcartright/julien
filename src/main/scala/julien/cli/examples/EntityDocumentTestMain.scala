package julien
package cli
package examples

import julien._
import access.Index
import retrieval._
import retrieval.Utils._

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
    println(keys.getKey + " " + keys.getKeyString)
  }
  println("docs: " + index.numDocuments + " cf:" + index.collectionLength)

  val query = args(0).split(" ").map(Term(_)).toSeq
  val modelFeatures = List.newBuilder[Feature]

  val sdm =
    Sum(List[Feature](
      Sum(children = query.map(a => Dirichlet(a, IndexLengths())),
        weight = 0.8),
      Sum(children = query.sliding(2, 1).map {
        p =>
          Dirichlet(OrderedWindow(1, p: _*), IndexLengths())
      }.toSeq,
        weight = 0.15),
      Sum(query.sliding(2, 1).map {
        p =>
          Dirichlet(UnorderedWindow(8, p: _*), IndexLengths())
      }.toSeq,
        weight = 0.05)
    ))
  modelFeatures += sdm

  // Make a processor to run it
  val model = modelFeatures.result
  val processor = SimpleProcessor()
  processor.add(model: _*)

  // Add the model to the processor
  processor.add(sdm)

  // run it and get results
  val results = processor.run()
  printResults(results, index)
}
