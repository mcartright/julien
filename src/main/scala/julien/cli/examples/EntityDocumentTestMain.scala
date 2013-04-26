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
  var index : Index = null

  val memIndex = Index.memory("/usr/dan/users4/jdalton/code/julien/src/main/resources/wiki-trectext-5.dat")
  val diskIndex = Index.disk("/usr/dan/users4/jdalton/code/julien/data/test-index")

  index = diskIndex

  val lengths = index.underlying.getIndexPart("lengths")
  val keys = lengths.keys()
  while (!keys.isDone) {
    keys.nextKey()
    println(keys.getKey + " " + keys.getKeyString)
  }
//  val docLens = lengths.getIterator(Utility."document")
  println("docs: " + memIndex.numDocuments + " cf:" + memIndex.collectionLength)

  val query = args(0).split(" ").map(Term(_))
  val modelFeatures = List.newBuilder[FeatureOp]

  val sdm =
    Combine(List[FeatureOp](
      Weight(Combine(query.map(a => Dirichlet(a,IndexLengths(), 50))), 0.8),
      Weight(Combine(query.sliding(2,1).map { p =>
        Dirichlet(OrderedWindow(1, p: _*), IndexLengths(), 50)
      }.toSeq), 0.15),
      Weight(Combine(query.sliding(2,1).map { p =>
        Dirichlet(UnorderedWindow(8, p: _*), IndexLengths(), 50)
      }.toSeq), 0.05)
    ))
   modelFeatures += sdm

  // Make a processor to run it
  val processor = SimpleProcessor()

  val model = modelFeatures.result

  val hooks = model.flatMap(m => m.iHooks).toSet
  hooks.foreach(_.attach(index))
  processor.add(model: _*)

  // Add the model to the processor
  processor.add(sdm)

  // run it and get results
  val results = processor.run


  printResults(results, diskIndex)
}
