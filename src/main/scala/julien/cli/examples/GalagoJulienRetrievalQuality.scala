package julien.cli.examples

import julien.retrieval._
import julien.access.Index
import julien.retrieval.Utils._
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import org.lemurproject.galago.core.retrieval

//import edu.umass.ciir.galago.{GalagoSearcher, GalagoQueryLib}

import org.lemurproject.galago.tupleflow.Parameters
import org.lemurproject.galago.core.retrieval.RetrievalFactory
import org.lemurproject.galago.core.retrieval.query.StructuredQuery
import org.lemurproject.galago.core.retrieval.ScoredDocument
/**
 * User: jdalton
 * Date: 4/29/13
 */
object GalagoJulienRetrievalQuality extends App {

  val queries = tsvToJulienQuery()

  val julResults = runJulienQueries(queries)
  val galagoResults = runGalagoQueries(queries)
  verifyRanks(queries, julResults, galagoResults)
  verifyEval("./data/qrels/robust04.qrels", galagoResults)
  verifyEval("./data/qrels/robust04.qrels", julResults)


  def verifyEval(qrelFile:String, results: Map[String, Seq[ScoredDocument]]) {
    val evalFormat = "%2$-16s%1$3s %3$6.5f";
    val evalScores = Evaluator.evaluate(qrelFile, results)
    evalScores._1.map(m => println(evalFormat.format("web", m._1, m._2)))
  }

  def verifyRanks(queries: Seq[String], r1:Map[String, Seq[ScoredDocument]], r2:Map[String, Seq[ScoredDocument]]) =  {
    for (q <- queries) {
      println(q)
      val queryId = q.split("\\s+")(0)
      val res1 = r1(queryId)
      val res2 = r2(queryId)

      if (res1.size != res2.size){
        throw new Exception("sizes don't match!")
      }
      val names1 = res1.map(_.documentName)
      val names2 = res2.map(_.documentName)

      val namesMatch = names1 equals names2
      if (!namesMatch) {

        for ( (name1,idx) <- names1.zipWithIndex) {
          if (!name1.equals(names2(idx))) {
            val r1 = res1(idx)
            val r2 = res2(idx)
            println("julien: " + r1.documentName + "," + r1.score + "galago: " + r2.documentName + "," + r2.score)
          }
        }
      }

    val scores1 = res1.map(_.score)
    val scores2 = res2.map(_.score)

    val scoresMatch = scores1 equals scores2
    if (!scoresMatch) {

      for ( (score1,idx) <- scores1.zipWithIndex) {
        if (!score1.equals(scores2(idx))) {
          val r1 = res1(idx)
          val r2 = res2(idx)
          println("Mismatch scores. julien: " + r1.documentName + "," + r1.score + " galago: " + r2.documentName + "," + r2.score)
        }
      }
    }

  }
  }

  def runJulienQueries(queries: Seq[String]) = {
    val index = Index.disk("/usr/dan/users4/jdalton/scratch2/thesis/document-retrieval-modeling/data/indices/robust04-jul")

   // val index = Index.disk("/usr/dan/users4/jdalton/code/julien/data/test-index-julien")

    val resultMap = HashMap[String, Seq[ScoredDocument]]()

    for (line <- queries) {
      val query = line.split("\\s+").drop(1).map(Term(_))
      println("Query:" + query.map(_.t).mkString(" "))
      val modelFeatures = List.newBuilder[FeatureOp]


      val ql = CombineNorm(query.map(a => Dirichlet(a, IndexLengths())))

      val sdm =
        CombineNorm(List[FeatureOp](
          CombineNorm(children = query.map(a => Dirichlet(a, IndexLengths())),
            weight = 0.8),
          CombineNorm(children = query.sliding(2, 1).map {
            p =>
              Dirichlet(OrderedWindow(1, p: _*), IndexLengths())
          }.toSeq,
            weight = 0.15),
          CombineNorm(query.sliding(2, 1).map {
            p =>
              Dirichlet(UnorderedWindow(8, p: _*), IndexLengths())
          }.toSeq,
            weight = 0.05)
        ))

      val queryModel = sdm

      println(queryModel.toString)

      modelFeatures += queryModel

      // Make a processor to run it
      val processor = SimpleProcessor()

      val model = modelFeatures.result

      val hooks = model.flatMap(m => m.iHooks).toSet
      hooks.foreach(_.attach(index))
     // processor.add(model: _*)

      // Add the model to the processor
      processor.add(queryModel)

      // run it and get results
      val results = processor.run()
      val galagoResults = for (r <- results.zipWithIndex) yield {
        val name = index.name(r._1.asInstanceOf[julien.retrieval.ScoredDocument].docid)
        new retrieval.ScoredDocument(name, r._2+1, r._1.asInstanceOf[julien.retrieval.ScoredDocument].score)
      }
      printResults(results, index)
      resultMap += (line.split("\\s+")(0) -> galagoResults)
    }
    resultMap.toMap
  }

  def runGalagoQueries(lines: Seq[String]) = {

    val conceptIndexParams = new Parameters()
    conceptIndexParams.set("mu", 1500)
  //  conceptIndexParams.set("index", "/usr/dan/users4/jdalton/code/julien/data/test-index-galago34")
   conceptIndexParams.set("index", "/usr/dan/users4/jdalton/scratch2/thesis/document-retrieval-modeling/data/indices/robust04-g34")
    conceptIndexParams.set("requested", 100)
    conceptIndexParams.set("stemming", false)
    conceptIndexParams.set("print", true)

    val resultMap = HashMap[String, Seq[ScoredDocument]]()

    val text2ConceptRetrieval = RetrievalFactory.instance(conceptIndexParams)
    for (queryLine <- lines) {
      val queryString = text2Query(queryLine.split("\\s+").drop(1).mkString(" "))

    //  val query = "#combine(" + queryString.mkString(" ") + ")"
      val query = "#seqdep(" + queryString.mkString(" ") + ")"
      val root = StructuredQuery.parse(query)
      val transformed = text2ConceptRetrieval.transformQuery(root, conceptIndexParams)
      println(transformed.toPrettyString)
      var results = text2ConceptRetrieval.runQuery(transformed, conceptIndexParams)
      if (results == null) {
        results = Array.empty[ScoredDocument]
      }
      resultMap += (queryLine.split("\\s+")(0) -> results)

      for (r <- results) {
        println(f"test ${r.documentName} ${r.score}%10.8f ${r.rank} galago")

      }
    }
    resultMap.toMap

  }

  def text2Query(query: String) = {
    query.replace("-", " ").split("\\s+").map(_.replaceAllLiterally("-", " ").replaceAll("[^a-zA-Z0-9]", "").toLowerCase).filter(_.length() > 1)
  }

  def tsvToJulienQuery() = {
    val queryFile = io.Source.fromFile("./data/test-queries/rob04.titles.tsv")
    val lines = queryFile.getLines()

    (lines take 2).toList
  }

}
