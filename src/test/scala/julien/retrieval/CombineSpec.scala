package julien
package retrieval

import org.scalatest._
import org.scalamock.scalatest.proxy.MockFactory

class CombineSpec extends FlatSpec with MockFactory {
  def fixture = new {
    val mock1 = mock[FeatureOp]
    val mock2 = mock[FeatureOp]
    val mock3 = mock[FeatureOp]

    mock1.expects('eval)
  }

  "A combine operator" should "by default sum its children" in {

  }
  it should "accept other viable reduce functions" in (pending)
  it should "return the passed in feature ops as children" in (pending)
  it should "return the set union view of its children" in (pending)

}
