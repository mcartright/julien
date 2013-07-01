package julien
package retrieval
package processor

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers._
import julien.eval._
import java.io.File
import julien.galago.tupleflow.Utility
import julien.retrieval.processor._
import scala.collection.BufferedIterator
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random
import scala.math._
import scala.collection.JavaConversions._

class CompareProcessorSpec
    extends FlatSpec
    with BeforeAndAfterEach {
  type SD = ScoredDocument
  type QPFactory =
    (Feature, Accumulator[SD]) => SingleQueryProcessor[SD]

  // Just pulling in this one function
  import julien.time

  private var readyToRun: Boolean = false
  private var cMap: Map[String, Any] = Map.empty
  private var julienPath: String = null
  private var queries: Map[String, String] = Map.empty
  private var qrels: QueryJudgmentSet = null
  private var sampleRate: Double = 0.1
  //private val vocabulary = collection.mutable.ListBuffer[String]()

  var julienIndex: Index = null

  override def beforeEach(td: TestData) {
    cMap = td.configMap
    readyToRun = if (!cMap.contains("procDir")) false
    else checkConfiguration(cMap("procDir").asInstanceOf[String])
    if (config.contains("qSampleRate"))
      sampleRate = config("qSampleRate").asInstanceOf[String].toDouble
    if (readyToRun) {
      julienIndex = Index.disk(julienPath)
    }
  }

  override def afterEach() {
    if (readyToRun) {
      julienIndex.close
    }
  }

  def config = cMap
  private def checkConfiguration(dir: String): Boolean = {
    try {
      if (julienPath == null) {
        // Get index path
        julienPath = new File(dir, "julien").getAbsolutePath
        // test that it's actually there
        val jtest = Index.disk(julienPath)
        jtest.close
      }
      true
    } catch {
      // This will be clear because we're skipping all the tests
      case e: Exception => { println(s"Whoops: $e"); false }
    }
  }

  lazy val termSample: Seq[String] = {
    val buffer = ListBuffer[String]()
    val iter = julienIndex.vocabulary().iterator.buffered
    while (iter.hasNext) {
      if (Random.nextDouble <= sampleRate) buffer += iter.head
      iter.next
    }
    buffer.result
  }

  def getSampleQueries(range: Int, low: Int): ListBuffer[Seq[String]] = {
    val queries = ListBuffer[Seq[String]]()
    var localSample = Random.shuffle(termSample)
    while (!localSample.isEmpty) {
      val desiredSize = min(localSample.length, Random.nextInt(range)+low)
      val (cut, leftover) = localSample splitAt desiredSize
      queries += cut
      localSample = leftover
    }
    queries
  }

  def compareRetrievals[T](
    p1Factory: QPFactory,
    p1Str: String,
    p2Factory: QPFactory,
    p2Str: String
  ) {
    // Generate queries
    val queries = getSampleQueries(6, 4)

    var p1TotalTime = 0.0D
    var p2TotalTime = 0.0D
    var numRun = 0
    val requested = 100
    // processor 1 - do the whole batch
    for ((query, idx) <- queries.zipWithIndex; if (query.size > 1)) {
      val formatted = bow(query, Dirichlet.apply)(julienIndex)
      val acc = DefaultAccumulator[SD](requested)
      val (ignoredResult, singleTime) = time {
        val r = p1Factory(formatted, acc).run()
        r
      }
      p1TotalTime += (singleTime.toDouble / 1000000.0)
      numRun += 1
    }

    // processor 2
    for ((query, idx) <- queries.zipWithIndex; if (query.size > 1)) {
      val formatted = bow(query, Dirichlet.apply)(julienIndex)
      val acc = DefaultAccumulator[SD](requested)
      val (ignoredResult, singleTime) = time {
        val r = p2Factory(formatted, acc).run()
        r
      }
      p2TotalTime += (singleTime.toDouble / 1000000.0)
    }

    // Report times
    val p1Avg = p1TotalTime.toDouble / numRun.toDouble
    val p2Avg = p2TotalTime.toDouble / numRun.toDouble
    info(s"queries run: $numRun")
    info(f"$p1Str avg: ${p1Avg}%3.6f ms, ($p1TotalTime ms)")
    info(f"$p2Str avg: ${p2Avg}%3.6f ms, ($p2TotalTime ms)")
  }

  it should "be slower on average than Maxscore for simple queries" in {
    if (!readyToRun) cancel("'procDir' was not defined.")
    compareRetrievals(
      SimpleProcessor.apply _,
      "Simple",
      MaxscoreProcessor.apply _,
      "Maxscore"
    )
  }

  it should "be slower on average than WAND for simple queries" in {
    if (!readyToRun) cancel("'procDir' was not defined.")
    compareRetrievals(
      SimpleProcessor.apply _,
      "Simple",
      WeakANDProcessor.apply _,
      "WAND"
    )
  }
}
