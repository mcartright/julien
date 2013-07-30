package julien
package retrieval

import org.scalatest._
import org.scalamock.scalatest.proxy.MockFactory
import julien._

import galago.core.util.ExtentArray
class UnorderedWindowSpec extends FlatSpec with MockFactory {

  def fixture = new {
    val mock1 = mock[PositionStatsView]
    val mock2 = mock[PositionStatsView]
    val mock3 = mock[PositionStatsView]
  }

  "An unordered window" should
  "require all children to be from the same index" in (pending)

  it should
  "complain if given a window size < 1 or window size is < # iterators" in {
    val f = fixture
    import f._

    val pos1 = Array(1,20)
    val pos2 = Array(21)
    val pos3 = Array(2, 19)

    val p1 = new ExtentArray(pos1)
    val p2 = new ExtentArray(pos2)
    val p3 = new ExtentArray(pos3)

    mock1.expects('positions)().returning(p1) noMoreThanTwice()
    mock2.expects('positions)().returning(p2) noMoreThanTwice()
    mock3.expects('positions)().returning(p3) noMoreThanTwice()

    intercept[AssertionError] {
      val uw = UnorderedWindow(1, mock1, mock2, mock3)
    }

    intercept[AssertionError] {
      val uw = UnorderedWindow(2, mock1, mock2, mock3)
    }
  }

  it should "correctly count the number of windows" in {
    val f = fixture
    import f._

    val pos1 = Array(1,20)
    val pos2 = Array(21)
    val pos3 = Array(2, 19)

    val p1 = new ExtentArray(pos1)
    val p2 = new ExtentArray(pos2)
    val p3 = new ExtentArray(pos3)

    mock1.expects('positions)(10).returning(p1)
    mock2.expects('positions)(10).returning(p2)
    mock3.expects('positions)(10).returning(p3)

    val uw = UnorderedWindow(3, mock1, mock2, mock3)

    val expected = new ExtentArray(Array(19), Array(21))
    val hits = uw.positions(10)
    expectResult(expected) {hits}
  }

  it should "correctly count the number of windows non-adjacent" in {
    val f = fixture
    import f._

    val pos1 = Array(6,24)
    val pos2 = Array(22)

    val p1 = new ExtentArray(pos1)
    val p2 = new ExtentArray(pos2)

    val id = 12
    mock1.expects('positions)(12).returning(p1)
    mock2.expects('positions)(12).returning(p2)

    val uw = UnorderedWindow(8, mock1, mock2)

    val expected = new ExtentArray(Array(22), Array(24))
    val hits = uw.positions(id)
    expectResult(expected) {hits}
  }

  it should "correctly return no hits" in {
    val f = fixture
    import f._

    val pos1 = Array(24,26)
    val pos2 = Array.empty[Int]

    val p1 = new ExtentArray(pos1)
    val p2 = new ExtentArray(pos2)

    val id = 50
    mock1.expects('positions)(id).returning(p1)
    mock2.expects('positions)(id).returning(p2)

    val uw = UnorderedWindow(8, mock1, mock2)

    val expected = new ExtentArray(Array.empty[Int])
    val hits = uw.positions(id)
    expectResult(expected) {hits}
  }
}
