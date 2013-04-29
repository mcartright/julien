package julien
package retrieval

import org.scalatest._
import java.io.File
import org.lemurproject.galago.tupleflow.{Utility,Parameters}
import java.util.logging.{Level,Logger}

trait SimpleProcessorBehavior { this: FlatSpec =>
  def index: Index
  def tmpForInput: File
  def indexParams: Parameters

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
      val index2 = Index.memory(tmpForInput.getAbsolutePath, "all", indexParams)
      proc add index2

      // And try to execute it
      proc add Dirichlet(Term("the"), IndexLengths())
      intercept[AssertionError] {
        val result = proc.run(DefaultAccumulator[ScoredDocument]())
      }
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
  val tmpForInput: File =
    new File(Utility.createTemporary.getAbsolutePath + ".gz")
  var index: Index = null
  val indexParams = new Parameters()

  override def beforeAll() {
    Logger.getLogger("").setLevel(Level.OFF)

    // Extract resource - small 5 doc collection for correctness testing
    val istream = getClass.getResourceAsStream("/wikisample.gz")
    assert (istream != null)
    Utility.copyStreamToFile(istream, tmpForInput)
    val parserParams = new Parameters()
    parserParams.set("filetype", "wikiwex")
    indexParams.set("parser", parserParams)
    indexParams.set("filetype", "wikiwex")
    index = Index.memory(tmpForInput.getAbsolutePath, "all", indexParams)
  }

  override def afterAll() {
    index.close
    tmpForInput.delete()
  }

  // These are to create factory functions to pass into
  // the behavior trait - each test needs its own way to
  // generate a fresh processor
  def simpleProc = SimpleProcessor()
  def maxProc = MaxscoreProcessor()
  def wandProc = WeakANDProcessor()

  "The SimpleProcessor" should
  behave like aSimpleProcessor(simpleProc)
  "The MaxcoreProcessor" should
  behave like aSimpleProcessor(maxProc)
  "The WeakANDProcessor" should
  behave like aSimpleProcessor(wandProc)

  it should "iterate over and score every candidate document (stupidly)" in (pending)
}
