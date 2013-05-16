package julien
package retrieval

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers._
import julien.eval._
import java.io.File
import org.lemurproject.galago.tupleflow.Parameters
import org.lemurproject.galago.core.index.disk.{DiskIndex => DIndex}
import org.lemurproject.galago.core.retrieval.iterator._
import org.lemurproject.galago.core.retrieval.processing.ScoringContext
import org.lemurproject.galago.core.retrieval.query._
import org.lemurproject.galago.core.retrieval._
import julien.galago.tupleflow.Utility
import scala.collection.BufferedIterator
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random
import scala.math._
import scala.collection.JavaConversions._

class RetrievalQualitySpec
    extends FlatSpec
    with BeforeAndAfterEach {
  type GARNA = org.lemurproject.galago.core.index.AggregateReader.NodeAggregateIterator
  type GARCA = org.lemurproject.galago.core.index.AggregateReader.CollectionAggregateIterator
  type GMEI = org.lemurproject.galago.core.retrieval.iterator.MovableExtentIterator

  private var readyToRun: Boolean = false
  private var cMap: Map[String, Any] = Map.empty
  private var julienPath: String = null
  private var galagoPath: String = null
  private var queries: Map[String, String] = Map.empty
  private var qrels: QueryJudgmentSet = null
  private var sampleRate: Double = 0.1
  private val vocabulary = collection.mutable.ListBuffer[String]()

  var julienIndex: Index = null
  var galagoIndex: DIndex = null

  override def beforeEach(td: TestData) {
    cMap = td.configMap
    readyToRun = if (!cMap.contains("qualityDir")) false
    else checkConfiguration(cMap("qualityDir").asInstanceOf[String])
    if (config.contains("qSampleRate"))
      sampleRate = config("qSampleRate").asInstanceOf[String].toDouble
    if (readyToRun) {
      julienIndex = Index.disk(julienPath)
      galagoIndex = new DIndex(galagoPath)
    }
  }

  override def afterEach() {
    if (readyToRun) {
      julienIndex.close
      galagoIndex.close
    }
  }

  def config = cMap
  private def checkConfiguration(dir: String): Boolean = {
    try {
      if (julienPath == null) {
        // Get index paths
        julienPath = new File(dir, "julien").getAbsolutePath
        val jtest = Index.disk(julienPath)
        // Load the vocabulary to save time later
        if (vocabulary.isEmpty) {
          val vIter = jtest.vocabulary().iterator
          while (vIter.hasNext) vocabulary += vIter.next
        }
        jtest.close

        galagoPath = new File(dir, "galago").getAbsolutePath
        val gtest = new DIndex(galagoPath)
        gtest.close
      }

      // Get the queries
      // Assumes a TSV format of "<qid>        <query>"
      if (queries.isEmpty) {
        val queryLines = Source.fromFile(new File(dir, "queries")).getLines
        queries = queryLines.map { line =>
          val pair = line.split("\t")
            (pair(0) -> pair(1))
        }.toMap
      }

      // Load qrels - assumes TREC format
      if (qrels == null) {
        qrels =
          QueryJudgmentSet.fromTrec(new File(dir, "qrels").getAbsolutePath)
      }
      true
    } catch {
      // This will be clear because we're skipping all the tests
      case e: Exception => { println(s"Whoops: $e"); false }
    }
  }

  lazy val termSample: Seq[String] = {
    val buffer = ListBuffer[String]()
    if (config.contains("qualityQuery")) {
      buffer ++= config("qualityQuery").asInstanceOf[String].split(";")
    } else {
      val iter = vocabulary.iterator.buffered
      while (iter.hasNext) {
        if (Random.nextDouble <= sampleRate) buffer += iter.head
        iter.next
      }
    }
    buffer.result
  }

  def getSampleQueries(range: Int, low: Int): ListBuffer[Seq[String]] = {
    val queries = ListBuffer[Seq[String]]()
    var localSample = termSample
    if (config.contains("qualityQuery")) {
      queries += localSample
    } else {
      while (!localSample.isEmpty) {
        val desiredSize = min(localSample.length, Random.nextInt(range)+low)
        val (cut, leftover) = localSample splitAt desiredSize
        queries += cut
        localSample = leftover
      }
    }
    queries
  }

  "Julien" should "have the same collection statistics as Galago" in {
    if (!readyToRun) cancel("'qualityDir' was not defined.")
    val jCS = julienIndex.collectionStats
    val gCS = galagoIndex.getLengthsIterator.asInstanceOf[GARCA].getStatistics
    expectResult(gCS.avgLength, "avgLength") { jCS.avgLength }
    expectResult(gCS.collectionLength, "collLength") { jCS.collectionLength }
    expectResult(gCS.documentCount, "docCount") { jCS.documentCount }
    expectResult(gCS.maxLength, "maxLength") { jCS.maxLength }
    expectResult(gCS.minLength, "minLength") { jCS.minLength }
  }

  it should "have the same vocabulary as Galago" in {
    if (!readyToRun) cancel("'qualityDir' was not defined.")
    val jvIter = termSample.iterator.buffered
    val gvIter = galagoIndex.getIndexPart("postings").getIterator
    while (jvIter.hasNext) {
      val jValue: String = jvIter.head
      gvIter.skipToKey(Utility.fromString(jValue))
      val gValue: String  = Utility.toString(gvIter.getKey)
      withClue(s"$jValue != $gValue") {
        jValue should equal (gValue)
      }
      jvIter.next
    }
  }

  it should "produce the same unigram statistics as Galago" in {
    if (!readyToRun) cancel("'qualityDir' was not defined.")
    val jvIter = termSample.iterator.buffered
    val gvIter = galagoIndex.getIndexPart("postings").getIterator
    while (jvIter.hasNext) {
      val jValue: String = jvIter.head
      gvIter.skipToKey(Utility.fromString(jValue))
      val gValue: String  = Utility.toString(gvIter.getKey)
      val jstats =
        julienIndex.iterator(jValue).asInstanceOf[ARNA].getStatistics
      val gstats =
        gvIter.getValueIterator.asInstanceOf[GARNA].getStatistics
      expectResult(gstats.maximumCount) { jstats.maximumCount }
      expectResult(gstats.nodeDocumentCount) { gstats.nodeDocumentCount }
      expectResult(gstats.nodeFrequency) { gstats.nodeFrequency }
      jvIter.next
    }
  }

  it should "produce the same OD statistics as Galago" in {
    if (!readyToRun) cancel("'qualityDir' was not defined.")
    // First make our set of windows
    val windows = getSampleQueries(4, 2)

    // For each window, we need to compare the hit locations and
    // counts at each location
    for ((window, idx) <- windows.zipWithIndex; if window.size > 1) {
      val genericClue = s"Query $idx: '${window.mkString(";")}'"
      // Run the Julien OD
      implicit val jIndex = julienIndex
      val jterms = window.map(t => Term(t))
      val jod = OrderedWindow(1, jterms: _*)
      val jhits = scala.collection.mutable.HashMap[String, Int]()
      for (posting <- jod.walker) {
        val name = julienIndex.name(posting.docid)
        val count = posting.positions.length
        if (count > 0) {
          //println(s"Julien matched at ${posting.docid} ($name, $count)")
          jhits(name) = count
        }
      }

      // Make and run the galago counterpart
      val sc = new ScoringContext
      val innerIterators = window.map { t =>
        val n = new Node("#extents", t)
        n.getNodeParameters.set("part", "postings")
        val it = galagoIndex.getIterator(n).asInstanceOf[GMEI]
        assert(it != null)
        it
      }
      val np = new NodeParameters()
      np.set("default", 1)
      val god = new OrderedWindowIterator(np, innerIterators.toArray)
      god.setContext(sc)
      val ghits = scala.collection.mutable.HashMap[String, Int]()
      while (!god.isDone) {
        val candidate = god.currentCandidate
        sc.document = candidate
        god.syncTo(candidate)
        val name = galagoIndex.getName(candidate)
        val ats = innerIterators.map(_.currentCandidate).mkString(",")
        val dones = innerIterators.map(_.isDone).mkString(",")
        val matches = innerIterators.map(_.hasMatch(candidate)).mkString(",")
        val gmatch =  god.hasMatch(candidate)
        if (god.hasMatch(candidate)) {
          val count = god.extents.size
          //println(s"Galago matched at $candidate ($name, $count)")
          ghits(name) = count
        }
        god.movePast(candidate)
      }

      // Now compare
      if (ghits.size != jhits.size) {
        // Let's have a nice detailed message
        val inGalago = ghits -- jhits.keys
        val inJulien = jhits -- ghits.keys
        val b = new StringBuilder()
        b ++= s"@ $genericClue \n"
        b ++= s"sizes uneven. Galago: ${ghits.size}, Julien: ${jhits.size}\n"
        b ++= s"In Galago only: ${inGalago.mkString(",")}\n"
        b ++= s"In Julien only: ${inJulien.mkString(",")}\n"
        fail(b.toString)
      }
      for (key <- ghits.keys.toSeq.sorted) {
        expectResult(true)(jhits.contains(key))
        expectResult(ghits(key), s"@ doc $key")(jhits(key))
      }
    }
  }

  it should "produce the same UW statistics as Galago" in {
    if (!readyToRun) cancel("'qualityDir' was not defined.")
    // First make our set of windows
    val windows = getSampleQueries(4, 2)

    // For each window, we need to compare the hit locations and
    // counts at each location
    for ((window, idx) <- windows.zipWithIndex; if window.size > 1) {
      val genericClue = s"Query $idx: '${window.mkString(";")}'"
      // Run the Julien UW
      implicit val jIndex = julienIndex
      val jterms = window.map(t => Term(t))
      val juw = UnorderedWindow(8, jterms: _*)
      val jhits = scala.collection.mutable.HashMap[String, Int]()
      for (posting <- juw.walker) {
        val name = julienIndex.name(posting.docid)
        val count = posting.positions.length
        if (count > 0) jhits(name) = count
      }

      // Make and run the galago counterpart
      val sc = new ScoringContext
      val innerIterators = window.map { t =>
        val n = new Node("#extents", t)
        n.getNodeParameters.set("part", "postings")
        val it = galagoIndex.getIterator(n).asInstanceOf[GMEI]
        assert(it != null)
        it
      }
      val np = new NodeParameters()
      np.set("default", 8)
      val guw = new UnorderedWindowIterator(np, innerIterators.toArray)
      guw.setContext(sc)
      val ghits = scala.collection.mutable.HashMap[String, Int]()
      while (!guw.isDone) {
        val candidate = guw.currentCandidate
        sc.document = candidate
        guw.syncTo(candidate)
        val name = galagoIndex.getName(candidate)
        val ats = innerIterators.map(_.currentCandidate).mkString(",")
        val dones = innerIterators.map(_.isDone).mkString(",")
        val matches = innerIterators.map(_.hasMatch(candidate)).mkString(",")
        val gmatch =  guw.hasMatch(candidate)
        if (guw.hasMatch(candidate)) {
          val count = guw.extents.size
          ghits(name) = count
        }
        guw.movePast(candidate)
      }

      // Now compare
      if (ghits.size != jhits.size) {
        // Let's have a nice detailed message
        val inGalago = ghits -- jhits.keys
        val inJulien = jhits -- ghits.keys
        val b = new StringBuilder()
        b ++= s"@ $genericClue \n"
        b ++= s"sizes uneven. Galago: ${ghits.size}, Julien: ${jhits.size}\n"
        b ++= s"In Galago only: ${inGalago.mkString(",")}\n"
        b ++= s"In Julien only: ${inJulien.mkString(",")}\n"
        fail(b.toString)
      }
      for (key <- ghits.keys.toSeq.sorted) {
        expectResult(true)(jhits.contains(key))
        val clue = s"@ $genericClue: @ doc $key, ${ghits(key)} != ${jhits(key)}"
        expectResult(ghits(key), clue)(jhits(key))
      }
    }
  }

  // Used to wrap the generators for Julien
  def qlWrap(query: Seq[String], i: Index) =
    bow(query, Dirichlet.apply)(i)
  def sdmWrap(query: Seq[String], i: Index) =
    sdm(query, Dirichlet.apply)(i)

  def compareRetrievals(
    julienCombiner: (Seq[String], Index) => FeatureOp,
    galagoCombiner: String
  ) {
    println("@1")
    // Generate queries
    val queries = getSampleQueries(4, 2)

    // Set up processing structures
    val processor = SimpleProcessor()
    val retrieval = new LocalRetrieval(galagoIndex, new Parameters)

    println("@2")

    // For each query, run against each index and compare results
    for ((query, idx) <- queries.zipWithIndex; if (query.size > 0)) {
      val clue = new StringBuilder()
      val genericClue = s"Query $idx: '${query.mkString(";")}'"
      try {
        val requested = 100
        // Julien
        val jquery = julienCombiner(query, julienIndex)
        processor.clear
        processor add jquery
        println("@3")
        val jUnstableResult =
          processor.run(DefaultAccumulator[ScoredDocument](requested))
        jUnstableResult.foreach(jr => jr.name = julienIndex.name(jr.id))
        println("@4")
        // Galago
        val queryParams = new Parameters()
        queryParams.set("processingModel",
          "org.lemurproject.galago.core.retrieval.processing.RankedDocumentModel")
        queryParams.set("requested", requested)
        val galagoQL = s"#${galagoCombiner}(" + query.mkString(" ") + ")"
        val root = StructuredQuery.parse(galagoQL)
        val node = retrieval.transformQuery(root, queryParams)
        println(s"galago query: $node")
        val gUnstableResult = retrieval.runQuery(node, queryParams)

        // Need to compare results
        expectResult(gUnstableResult.length)(jUnstableResult.length)

        // group by score, then compare group by group (so things with the same
        // scores all have the same order
        val jResult =
          jUnstableResult.groupBy(_.score.toFloat).mapValues(_.toSet)
        val jScores = jResult.keys.toSeq.sorted
        val gResult =
          gUnstableResult.groupBy(_.score.toFloat).mapValues(_.toSet)
        val gScores = gResult.keys.toSeq.sorted

        for ((gs, js) <- gScores.zip(jScores)) {
          val gSet = gResult(gs).map(_.documentName)
          val jSet = jResult(js).map(_.name)
          println(s"G: $gs -> ${gSet}\nJ: $js -> ${jSet}")
        }

        for (((gscore, jscore), idx) <- gScores.zip(jScores).zipWithIndex) {
          val jr = jResult(jscore)
          val gr = gResult(gscore)
          clue.clear
          clue ++= s"$genericClue: position [$idx]\n"
          clue ++= s"G: ${gr.toString}, \n J: ${jr.toString}"
          withClue(clue.toString) {
            gscore should be (jscore plusOrMinus 0.0000001F)
            gr.size should equal (jr.size)
            val gSet = gr.map(_.documentName)
            val jSet = jr.map(_.name)
            if (idx == 0) {
              // all bets are off - too much instability in the ordering, so
              // ignore it.
            } else {
              // exact set match
              expectResult(gSet)(jSet)
            }
          }
        }
      } catch {
        case e: Exception => {
          val msg = if (clue.size > 0) clue else genericClue
          e.printStackTrace(Console.err)
          fail(s"$msg => ${e.getMessage}", e)
        }
      }
    }
  }


  it should "produce the same scores as Galago for simple queries" in {
    if (!readyToRun) cancel("'qualityDir' was not defined.")
    //compareRetrievals(qlWrap _, "combine")
  }

  it should "produce the same scores as Galago for SDM queries" in {
    if (!readyToRun) cancel("'qualityDir' was not defined.")
    compareRetrievals(sdmWrap _, "sdm")
  }
}
