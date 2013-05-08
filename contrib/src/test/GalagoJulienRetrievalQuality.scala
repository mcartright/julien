package julien.cli.examples

import julien.retrieval._
import julien.access.Index
import collection.mutable.{ArrayBuffer, HashMap}
import org.lemurproject.galago.core.retrieval
import retrieval.processing.RankedDocumentModel
import julien.eval.Evaluator
import julien.InternalId

//import edu.umass.ciir.galago.{GalagoSearcher, GalagoQueryLib}

import org.lemurproject.galago.tupleflow.Parameters
import retrieval.{LocalRetrieval, RetrievalFactory}
import org.lemurproject.galago.core.retrieval.query.StructuredQuery

/**
 * User: jdalton
 * Date: 4/29/13
 */
object GalagoJulienRetrievalQuality extends App {

 //  Thread.sleep(10000)
  val myTestQueries = Array("1 wood construction")

  val queries = tsvToJulienQuery()

  val julResults = runJulienQueries(queries)
  val galagoResults = runGalagoQueries(queries)
  // verifyRanks(queries, julResults, galagoResults)
 //  verifyEval("./data/qrels/robust04.qrels", galagoResults)
 //  verifyEval("./data/qrels/robust04.qrels", julResults)


  def verifyEval(qrelFile: String, results: Map[String, Seq[ScoredDocument]]) {
    val evalFormat = "%2$-16s%1$3s %3$6.5f";
    val evalScores = Evaluator.evaluate(qrelFile, results)
    evalScores._1.map(m => println(evalFormat.format("web", m._1, m._2)))
  }

  def verifyRanks(queries: Seq[String], r1: Map[String, Seq[ScoredDocument]], r2: Map[String, Seq[ScoredDocument]]) = {
    for (q <- queries) {
      println(q)
      val queryId = q.split("\\s+")(0)
      val res1 = r1(queryId)
      val res2 = r2(queryId)

      if (res1.size != res2.size) {
        throw new Exception("sizes don't match!")
      }
      val names1 = res1.map(_.name)
      val names2 = res2.map(_.name)

      val namesMatch = names1 equals names2
      if (!namesMatch) {

        for ((name1, idx) <- names1.zipWithIndex) {
          if (!name1.equals(names2(idx))) {
            val r1 = res1(idx)
            val r2 = res2(idx)
            println("julien: " + r1.name + "," + r1.score + "galago: " + r2.name + "," + r2.score)
          }
        }
      }

      val scores1 = res1.map(_.score)
      val scores2 = res2.map(_.score)

      val scoresMatch = scores1 equals scores2
      if (!scoresMatch) {

        for ((score1, idx) <- scores1.zipWithIndex) {
          if (!((score1 - scores2(idx)).abs < 0.00001)) {
            val r1 = res1(idx)
            val r2 = res2(idx)
            println("Mismatch scores. julien: " + r1.name + "," + r1.score + " galago: " + r2.name + "," + r2.score)
          }
        }
      }

    }
  }

  def runJulienQueries(queries: Seq[String]) = {
    implicit val index = Index.memoryFromDisk("/usr/dan/users4/jdalton/scratch2/thesis/document-retrieval-modeling/data/indices/robust04-jul-11")

   // val index = Index.disk("/usr/dan/users4/jdalton/code/julien/data/test-index-julien")

    val resultMap = HashMap[String, Seq[ScoredDocument]]()

    val mu = 1269
    println("start julien")
    var start = System.currentTimeMillis()

    for (i <- 1 to 2) {

      if (i == 2) {
        start = System.currentTimeMillis()
      }

      for (line <- queries) {

        val query = line.split("\\s+").drop(1).map(Term(_))
        println("Query: raw: " + line + " processed: " + query.map(_.t).mkString(" "))
        val modelFeatures = List.newBuilder[FeatureOp]

        val useQl = false
        val queryModel = if (useQl || query.size == 1) {
          val ql = CombineNorm(query.map(a => Dirichlet(a, IndexLengths(), mu)))
          ql
        } else {

          val unigramWeight = 0.87264
          val odWeight = 0.07906
          val udWeight = 0.04829

          val children = new ArrayBuffer[FeatureOp]()
          children ++= query.map(a => Dirichlet(a, IndexLengths(), mu,  weight = (unigramWeight / query.length)))
          children ++= query.sliding(2, 1).map {p => Dirichlet(OrderedWindow(1, p: _*), IndexLengths(), mu, weight = (odWeight / (query.length-1)))}.toSeq
          children ++= query.sliding(2, 1).map {p => Dirichlet(UnorderedWindow(8, p: _*), IndexLengths(), mu, weight = (udWeight / (query.length-1)))}.toSeq

          val sdm = CombineNorm(children)
          sdm
        }

        //  println(queryModel.toString)

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
//        val galagoResults = for (r <- results.zipWithIndex) yield {
//          val name = index.name(r._1.asInstanceOf[julien.retrieval.ScoredDocument].docid)
//          new retrieval.ScoredDocument(name, r._2 + 1, r._1.asInstanceOf[julien.retrieval.ScoredDocument].score)
//        }
        // printResults(results, index)
        resultMap += (line.split("\\s+")(0) -> results)
      }
    }
    val end = System.currentTimeMillis()
    val dif = end - start
    println("Julien Duration: " + dif)

    resultMap.toMap
  }

  def runGalagoQueries(lines: Seq[String]) = {

    val p = new Parameters()
    p.set("mu", 1269)
    p.set("uniw", 0.87264)
    p.set("odw", 0.07906)
    p.set("uww", 0.04829)
   // p.set("index", "/usr/dan/users4/jdalton/code/julien/data/test-index-galago34")
    p.set("index", "/usr/dan/users4/jdalton/scratch2/thesis/document-retrieval-modeling/data/indices/robust04-g34")
    p.set("requested", 1000)
 //   p.set("annotate", true)
    p.set("stemming", false)
    p.set("deltaReady", false)
    // conceptIndexParams.set("print", true)

    val resultMap = HashMap[String, Seq[ScoredDocument]]()

    val text2ConceptRetrieval = RetrievalFactory.instance(p).asInstanceOf[LocalRetrieval]
    println("starting galago queries")
    val start = System.currentTimeMillis()
    for (i <- 1 to 1) {
      for (queryLine <- lines) {
        val queryString = text2Query(queryLine.split("\\s+").drop(1).mkString(" "))

       //val query = "#combine(" + queryString.mkString(" ") + ")"
       // println(queryLine)
        val query = "#sdm(" + queryString.mkString(" ") + ")"
        val root = StructuredQuery.parse(query)
        val transformed = text2ConceptRetrieval.transformQuery(root, p)
        // println(transformed.toPrettyString)
        val proc = new RankedDocumentModel(text2ConceptRetrieval)
        var results = text2ConceptRetrieval.runQuery(transformed, p) //proc.execute(transformed, p)
        if (results == null) {
          results = Array.empty[org.lemurproject.galago.core.retrieval.ScoredDocument]
        }

        val julienResults = for ((r,idx) <- results.zipWithIndex) yield {
          //          val name = index.name(r._1.asInstanceOf[julien.retrieval.ScoredDocument].docid)
                    new ScoredDocument(new InternalId(-1), r.score, r.documentName, idx + 1)
                  }

        resultMap += (queryLine.split("\\s+")(0) -> julienResults)

//              for (r <- results) {
//                val name = text2ConceptRetrieval.getDocumentName(r.document)
//                println("document: " + name)
//                println(r.annotation.toString)
//                r.documentName = name
            //    println(f"test ${r.documentName} ${r.score}%10.8f ${r.rank} galago")

            //  }
      }
    }
    val end = System.currentTimeMillis()
    val dif = end - start
    println("Galago Duration: " + dif)
    resultMap.toMap

  }

  def text2Query(query: String) = {
    query.replace("-", " ").split("\\s+").map(_.replaceAllLiterally("-", " ").replaceAll("[^a-zA-Z0-9]", "").toLowerCase).filter(_.length() > 1)
  }

  def tsvToJulienQuery() = {
    val queryFile = io.Source.fromFile("./data/test-queries/rob04.titles.tsv")
    val lines = queryFile.getLines()

    val filtered = lines.toList //.filter(_.split("\\s+")(0).toInt > 600).toList
    filtered
  }

}
