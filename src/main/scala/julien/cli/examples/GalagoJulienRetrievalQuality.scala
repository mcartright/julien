package julien.cli.examples

import julien.retrieval._
import julien.access.Index
import julien.retrieval.Utils._

//import edu.umass.ciir.galago.{GalagoSearcher, GalagoQueryLib}
//import org.lemurproject.galago.tupleflow.Parameters
//import org.lemurproject.galago.core.retrieval.{Retrieval, RetrievalFactory}
//import org.lemurproject.galago.core.retrieval.query.{AnnotatedNode, StructuredQuery, Node}

/**
 * User: jdalton
 * Date: 4/29/13
 */
object GalagoJulienRetrievalQuality extends App {

  val queries = tsvToJulienQuery()

  runJulienQueries(queries)
//  runGalagoQueries(queries)

  def runJulienQueries(queries: Seq[String]) = {

    val index = Index.disk("/usr/dan/users4/jdalton/scratch2/thesis/document-retrieval-modeling/data/indices/robust04-jul")

    val queries = tsvToJulienQuery()
    for (line <- queries) {
      val query = line.split(" ").map(Term(_))
    val modelFeatures = List.newBuilder[FeatureOp]

      val sdm =
        Combine(List[FeatureOp](
          Combine(children = query.map(a => Dirichlet(a,IndexLengths())),
            weight = 0.8),
          Combine(children = query.sliding(2,1).map { p =>
            Dirichlet(OrderedWindow(1, p: _*), IndexLengths())
          }.toSeq,
            weight = 0.15),
          Combine(query.sliding(2,1).map { p =>
            Dirichlet(UnorderedWindow(8, p: _*), IndexLengths())
          }.toSeq,
            weight = 0.05)
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
    val results = processor.run()
    printResults(results, index)
    }
  }

//  def runGalagoQueries(lines:Seq[String]) {
//
//    val conceptIndexParams = new Parameters()
//    conceptIndexParams.set("mu", 1500)
//    conceptIndexParams.set("index", "/usr/dan/users4/jdalton/scratch2/thesis/document-retrieval-modeling/data/indices/robust04-jul")
//
//    val text2ConceptRetrieval = RetrievalFactory.instance(conceptIndexParams)
//    for (queryLine <- lines) {
//    val queryString = text2Query(queryLine)
//    val query = "#seqdep(" + queryString.mkString(" ") + ")"
//    val root = StructuredQuery.parse(query)
//    val transformed = text2ConceptRetrieval.transformQuery(root, conceptIndexParams)
//    val results = text2ConceptRetrieval.runQuery(transformed, conceptIndexParams)
//    for (r <- results) {
//
//      println(f"test ${r.documentName} ${r.score}%10.8f ${r.rank} galago")
//
//    }
//    }
//  }

  def text2Query(query:String) = {
    query.replace("-", " ").split("\\s+").map(_.replaceAllLiterally("-"," ").replaceAll("[^a-zA-Z0-9]", "").toLowerCase).filter(_.length() > 1)
  }

  def tsvToJulienQuery() = {
  val queryFile = io.Source.fromFile("./data/test-queries/rob04.titles.tsv")
  val lines = queryFile.getLines()

    (lines take 2).toList
  }

}
