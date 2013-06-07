package julien
package retrieval

import org.scalatest._
import org.scalamock.scalatest.proxy.MockFactory

class CombineSpec extends FlatSpec with MockFactory {
  def fixture = new {
    val mock1 = mock[Feature]
    val mock2 = mock[Feature]
    val mock3 = mock[Feature]
  }

  "A sum operator" should "sum its children" in {
    val f = fixture
    import f._

    val v1 = 3.7
    val v2 = 12.9
    val v3 = 11.13
    val id = 5
    mock1.expects('eval)(id).returning(v1)
    mock1.expects('weight)().returning(1.0)
    mock2.expects('eval)(id).returning(v2)
    mock2.expects('weight)().returning(1.0)
    mock3.expects('eval)(id).returning(v3)
    mock3.expects('weight)().returning(1.0)
    val c = Sum(List(mock1, mock2, mock3))
    expectResult(v1+v2+v3) { c.eval(id) }
  }

  "A normalized sum" should "average of the weights of the children" in {
    val f = fixture
    import f._

    val id = 3
    val scores = List(0.98, 0.55, 0.78887)
    val weights = List(1.3, 4.8, 9.9)
    mock1.expects('eval)(id).returning(scores(0))
    mock1.expects('weight)().returning(weights(0)).noMoreThanTwice
    mock2.expects('eval)(id).returning(scores(1))
    mock2.expects('weight)().returning(weights(1)).noMoreThanTwice
    mock3.expects('eval)(id).returning(scores(2))
    mock3.expects('weight)().returning(weights(2)).noMoreThanTwice

    val sum = weights.sum
    val result = scores.zip(weights).foldLeft(0.0) { (s, pair) =>
      s + (pair._1 * pair._2 / sum)
    }

    val ns = NormalizedSum(children = List(mock1, mock2, mock3))
    expectResult(result) { ns.eval(id) }
  }

  it should "return the passed in feature ops as children" in {
    val f = fixture
    import f._

    val c = Combine(List(mock1, mock2, mock3))
    expectResult(3) { c.children.size }
    assert(c.children.exists(_ == mock1))
    assert(c.children.exists(_ == mock2))
    assert(c.children.exists(_ == mock3))
  }
}
