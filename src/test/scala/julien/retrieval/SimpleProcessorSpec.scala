package julien
package retrieval

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers._
import java.io.File
import julien.galago.tupleflow.{Utility,Parameters}
import java.util.logging.{Level,Logger}
import scala.util.Random
import julien.access.QuickIndexBuilder
import julien.eval.QueryResult

trait SimpleProcessorBehavior extends QuickIndexBuilder { this: FlatSpec =>
  def index: Index
  def vocabulary: Seq[String]
  val epsilon: Double = 1.0E-10
  def config: Map[String, Any]

  def getQueryTerms(): List[String] = config.contains("query") match {
    case true => config("query").asInstanceOf[String].split(";").toList
    case false => Random.shuffle(vocabulary).take(5).toList
  }

  def getQuery(terms: List[String]): Tuple2[FeatureOp, String] = {
    implicit val defaultIndex = index
    val models = List("BM25", "JM", "Dir")
    val chosen = if (config.contains("scorer"))
      config("scorer").asInstanceOf[String]
    else
      models(Random.nextInt(models.size))

    val l = IndexLengths()
    val query = chosen match {
      case "BM25" => Combine(terms.map(t => BM25(Term(t), l)))
      case "JM" => Combine(terms.map(t => JelinekMercer(Term(t), l)))
      case "Dir" => Combine(terms.map(t => Dirichlet(Term(t), l)))
    }
    (query, chosen)
  }


  // This is poorly named, but I just needed to factor out
  // some code.
  def anAccumulatorProcessor(
    ref: => QueryProcessor,
    pFactory: => QueryProcessor) {
    it should "have the same results as the SimpleProcessor" in {
      // Make a random 5-word query.
      val qterms = getQueryTerms()
      val (query, scorerName) = getQuery(qterms)
      val genericClue = s"query=${qterms.mkString(";")},scorer=$scorerName"

      // Do the simple run
      val sp = ref
      sp add query
      val simpleResults = sp.run(DefaultAccumulator[ScoredDocument](3))

      // Now do alternate run
      val alt = pFactory
      alt add query
      val altResults = alt.run(DefaultAccumulator[ScoredDocument](3))

      // And compare
      withClue(genericClue) {
        simpleResults.size should equal (altResults.size)
      }

      for ((result, idx) <- simpleResults.zipWithIndex) {
        withClue(s"@$idx, $result != ${altResults(idx)}, $genericClue") {
          altResults(idx).id should equal (result.id)
          altResults(idx).score should be (result.score plusOrMinus epsilon)
        }
      }
    }

    it should "return the same results when the accumulator is not full" in {
      // Make a random 5-word query.
      val qterms = getQueryTerms()
      val (query, scorerName) = getQuery(qterms)
      val genericClue = s"query=${qterms.mkString(";")},scorer=$scorerName"

      // Do the simple run
      val sp = ref
      sp add query
      val simpleResults = sp.run(DefaultAccumulator[ScoredDocument](10000))

      // Now do maxscore run
      val alt = pFactory
      alt add query
      val altResults = alt.run(DefaultAccumulator[ScoredDocument](10000))

      // And compare
      withClue(genericClue) {
        simpleResults.size should equal (altResults.size)
      }
      for ((result, idx) <- simpleResults.zipWithIndex) {
        withClue(s"@$idx, $result != ${altResults(idx)}, $genericClue") {
          altResults(idx).id should equal (result.id)
          altResults(idx).score should be (result.score plusOrMinus epsilon)
        }
      }
    }
  }

  def aSimpleProcessor(pFactory: => QueryProcessor) {
    it should "start with no models" in {
      expectResult(0) { pFactory.models.size }
    }

    it should "accept feature operators" in {
      implicit val implicitIndex = index
      val proc = pFactory
      val f1 = Dirichlet(Term("the"), IndexLengths())
      proc add f1
      val f2 = BM25(Term("white"), IndexLengths())
      proc add f2
      val f3 = JelinekMercer(Term("Owl"), IndexLengths())
      proc add f3
      val models = proc.models.toSet

      expectResult(true) { models(f1) }
      expectResult(true) { models(f2) }
      expectResult(true) { models(f3) }
    }

    it should "complain if executed with no models" in {
      val proc = pFactory
      intercept[AssertionError] {
        val results = proc.run(DefaultAccumulator[ScoredDocument]())
      }
    }
  }
}

class SimpleProcessorSpec
    extends FlatSpec
    with BeforeAndAfterAll
    with SimpleProcessorBehavior {
  var index: Index = null
  val vocabulary = collection.mutable.ListBuffer[String]()

  // Extracts the configuration map to use in the test. These are set
  // on the cli like "mvn test -Dconfig=<k1>=<v1>,<k2>=<v2>..."
  var cMap: Map[String, Any] = Map.empty
  def config = cMap
  override def run(testName: Option[String], args: Args): Status = {
    cMap = args.configMap
    super.run(testName, args)
  }

  override def beforeAll() {
    index = makeSampleMemory
    // let's get that vocab out too
    val vIter = index.vocabulary().iterator
    while (vIter.hasNext) { vocabulary += vIter.next }
  }

  override def afterAll() {
    index.close
    deleteSampleMemory
    deleteWiki5Memory
  }

  // These are to create factory functions to pass into
  // the behavior trait - each test needs its own way to
  // generate a fresh processor
  def simpleProc = SimpleProcessor()
  def maxProc = MaxscoreProcessor()
  def wandProc = WeakANDProcessor()

  "The SimpleProcessor" should
  behave like aSimpleProcessor(simpleProc)

  it should "iterate over and score every candidate document (stupidly)" in {
    val queryTerms =
      List("arrangement", "baptist", "goethe", "liberal", "october")
    val l = IndexLengths()(index)
    val query = Combine(queryTerms.map(t => TF(Term(t)(index), l)))
    val sp = simpleProc
    sp add query
    val acc = DefaultAccumulator[ScoredDocument](size = 1000)
    val results: QueryResult[ScoredDocument] = sp.run(acc)

    // Now let's do this by hand, and compare results
    if (l.isInstanceOf[Movable]) l.asInstanceOf[Movable].reset
    val counts = scala.collection.mutable.ListBuffer[ScoredDocument]()
    val iterators = queryTerms.map(t => index.iterator(t))
    while (iterators.exists(!_.isDone)) {
      val min = iterators.filterNot(_.isDone).map(_.currentCandidate).min
      iterators.foreach(_.syncTo(min))
      if (l.isInstanceOf[Movable]) l.asInstanceOf[Movable].moveTo(min)
      if (iterators.exists(_.hasMatch(min))) {
        val total = iterators.foldLeft(0.0) { (sum , iter) =>
          if (iter.hasMatch(min))
            sum + (iter.count.toDouble / l.length(min))
          else
            sum
        }
        val candidate = ScoredDocument(InternalId(min), total.toDouble)
        counts += candidate
      }
      iterators.foreach(_.movePast(min))
    }
    val sorted = counts.sorted

    // Now compare
    for ((result, idx) <- sorted.zipWithIndex) {
      withClue(s"@$idx, $result != ${results(idx)}") {
        result.id should equal (results(idx).id)
        results(idx).score should be (result.score plusOrMinus epsilon)
      }
    }
  }

  "The MaxcoreProcessor" should behave like aSimpleProcessor(maxProc)
  it should behave like anAccumulatorProcessor(simpleProc, maxProc)


  "The WeakANDProcessor" should behave like aSimpleProcessor(wandProc)
  it should behave like anAccumulatorProcessor(simpleProc, wandProc)

  "Maxscore and WeakAND" should
  behave like anAccumulatorProcessor(maxProc, wandProc)

}
