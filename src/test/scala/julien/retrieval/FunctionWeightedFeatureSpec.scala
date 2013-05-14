package julien
package retrieval

import org.scalatest._
import scala.language.reflectiveCalls

class FunctionWeightedFeatureSpec extends FlatSpec {
  def fixture = new {
    val swOp = new FunctionWeightedFeature with ChildlessOp {
      val views = Set.empty[ViewOp]
      def eval = 1.0
    }
  }
  "A function weight operator" should "provide a default value of 1.0" in {
    val f = fixture
    import f._
    expectResult(1.0) { swOp.weight }
  }

  it should "allow a new weight function to be set" in {
    val f = fixture
    import f._
    swOp.weight = () => 45.0
  }

  it should "use the new weight function after setting it" in {
    val f = fixture
    import f._
    import scala.collection.immutable.Queue

    // Because the Queue implementation is silly...
    val tmp = new {
      var q = Queue(3.0, 4.5, 99.0, 112.1)
      def apply: Double = {
        val result = q.dequeue
        q = result._2
        result._1
      }
    }
    expectResult(1.0) { swOp.weight }
    swOp.weight = tmp.apply
    expectResult(3.0) { swOp.weight }
    expectResult(4.5) { swOp.weight }
    expectResult(99.0) { swOp.weight }
    expectResult(112.1) { swOp.weight }
    intercept[NoSuchElementException] { swOp.weight }
  }

  it should "return the weight function on request" in {
    val f = fixture
    import f._

    val myScoreFunc = () => 13.3
    swOp.weight = myScoreFunc
    assert( myScoreFunc == swOp.weightFunction )
  }
}
