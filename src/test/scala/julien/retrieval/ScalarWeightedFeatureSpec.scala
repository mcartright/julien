package julien
package retrieval

import org.scalatest._

class ScalarWeightedFeatureSpec extends FlatSpec {
  def fixture = new {
    val swOp = new ScalarWeightedFeature with ChildlessOp {
      val views = Set.empty[ViewOp]
      def eval = 1.0
    }
  }
  "A scalar weight operator" should "provide a default value of 1.0" in {
    val f = fixture
    import f._
    expect(1.0) { swOp.weight }
  }

  it should "allow a new scalar value to be set" in {
    val f = fixture
    import f._
    swOp.weight = 45.0
  }

  it should "return the new scalar value after setting the new weight" in {
    val f = fixture
    import f._

    expect(1.0) { swOp.weight }
    val newWeight = scala.util.Random.nextDouble
    swOp.weight = newWeight
    expect(newWeight) { swOp.weight }
  }
}
