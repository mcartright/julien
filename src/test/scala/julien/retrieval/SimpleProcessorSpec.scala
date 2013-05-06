package julien
package retrieval

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers._
import java.io.File
import julien.galago.tupleflow.{Utility,Parameters}
import java.util.logging.{Level,Logger}
import scala.util.Random
import julien.access.QuickIndexBuilder

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
      sp add index
      sp add query
      val simpleResults = sp.run(DefaultAccumulator[ScoredDocument](3))

      // Now do alternate run
      val alt = pFactory
      alt add index
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
      sp add index
      sp add query
      val simpleResults = sp.run(DefaultAccumulator[ScoredDocument](10000))

      // Now do maxscore run
      val alt = pFactory
      alt add index
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

    it should "start with no indexes" in {
      expectResult(0) { pFactory.indexes.size }
    }

    it should "accept feature operators" in {
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

    it should "accept indexes" in {
      val proc = pFactory
      proc add index
      expectResult(1) { proc.indexes.size }
      expectResult(true) { proc.indexes.toSet(index) }
    }

    it should "accept the same index only once" in {
      val proc = pFactory
      proc add index
      proc add index
      expectResult(1) { proc.indexes.size }
    }

    it should "complain if executed with no models" in {
      val proc = pFactory
      proc add index
      intercept[AssertionError] {
        val results = proc.run(DefaultAccumulator[ScoredDocument]())
      }
    }

    it should "complain if executed with no indexes" in {
      val proc = pFactory
      proc add Dirichlet(Term("a"), IndexLengths())
      intercept[AssertionError] {
        proc.run(DefaultAccumulator[ScoredDocument]())
      }
    }

    it should "complain if more than 1 index is provided" in {
      val proc = pFactory
      proc add index

      // Let's just open another one
      val index2 = makeWiki5Memory
      proc add index2

      // And try to execute it
      proc add Dirichlet(Term("the"), IndexLengths())
      intercept[AssertionError] {
        val result = proc.run(DefaultAccumulator[ScoredDocument]())
      }
      deleteWiki5Memory
    }

    it should "automatically attach hooks if only 1 index is provided" in {
      val proc = pFactory
      val terms =
        "little red riding hood went to the store".split(" ").map(Term(_))
      val l = IndexLengths()
      val ql = Combine(terms.map(t => Dirichlet(t, l)))
      proc add ql
      proc add index
      expectResult(false) { ql.iHooks.exists(_.isAttached) }
      expectResult(true) { proc.validated }
      expectResult(true) { ql.iHooks.forall(_.isAttached) }
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
    val query = Combine(queryTerms.map(t => TermCount(t)))
    val sp = simpleProc
    sp add query
    sp add index
    val acc = DefaultAccumulator[ScoredDocument](size = 1000)
    val results: List[ScoredDocument] = sp.run(acc)

    // Now let's do this by hand, and compare results

    val counts = scala.collection.mutable.ListBuffer[ScoredDocument]()
    val iterators = queryTerms.map(t => index.iterator(t))
    while (iterators.exists(!_.isDone)) {
      val min = iterators.filterNot(_.isDone).map(_.currentCandidate).min
      iterators.foreach(_.syncTo(min))
      if (iterators.exists(_.hasMatch(min))) {
        val total = iterators.foldLeft(0) { (sum , iter) =>
          if (iter.hasMatch(min)) sum + iter.count else sum
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

  /*
  "Maxscore and WeakAND" should
  behave like anAccumulatorProcessor(maxProc, wandProc)
 */
}
