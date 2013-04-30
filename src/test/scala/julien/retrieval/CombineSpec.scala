package julien
package retrieval

import org.scalatest._
import org.scalamock.scalatest.proxy.MockFactory

class CombineSpec extends FlatSpec with MockFactory {
  def fixture = new {
    val mock1 = mock[FeatureOp]
    val mock2 = mock[FeatureOp]
    val mock3 = mock[FeatureOp]
  }

  "A combine operator" should "by default sum its children" in {
    val f = fixture
    import f._

    val v1 = 3.7
    val v2 = 12.9
    val v3 = 11.13
    mock1.expects('eval)().returning(v1)
    mock1.expects('weight)().returning(1.0)
    mock2.expects('eval)().returning(v2)
    mock2.expects('weight)().returning(1.0)
    mock3.expects('eval)().returning(v3)
    mock3.expects('weight)().returning(1.0)
    val c = Combine(List(mock1, mock2, mock3))
    expect(v1+v2+v3) { c.eval }
  }

  it should "accept other viable reduce functions" in {
    val f = fixture
    import f._

    val v1 = 0.98
    val v2 = 0.55
    val v3 = 0.78887
    mock1.expects('eval)().returning(v1)
    mock2.expects('eval)().returning(v2)
    mock3.expects('eval)().returning(v3)
    val prod = (s: Seq[FeatureOp]) =>
    s.foldLeft(1.0)((s , feat) => s * feat.eval)
    val c = Combine(combiner = prod, children = List(mock1, mock2, mock3))
    expect(v1*v2*v3) { c.eval }
  }

  it should "return the passed in feature ops as children" in {
    val f = fixture
    import f._

    val c = Combine(List(mock1, mock2, mock3))
    expect(3) { c.children.size }
    assert(c.children.exists(_ == mock1))
    assert(c.children.exists(_ == mock2))
    assert(c.children.exists(_ == mock3))
  }

  it should "return the set union view of its children" in (pending)

}
