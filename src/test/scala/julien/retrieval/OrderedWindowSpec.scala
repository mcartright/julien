package julien
package retrieval

import org.scalatest._
import org.scalamock.scalatest.proxy.MockFactory
import julien._
import galago.core.util.ExtentArray

class OrderedWindowSpec extends FlatSpec with MockFactory {

  def fixture = new {
    val mock1 = mock[PositionStatsView]
    val mock2 = mock[PositionStatsView]
    val mock3 = mock[PositionStatsView]
  }

  "An ordered window" should
  "require all children to be from the same index" in (pending)

  it should "complain if given a window size < 1" in (pending)

  it should "correctly count the number of windows" in {
    val f = fixture
    import f._

    val pos1 = Array(1,20)
    val pos2 = Array(21)
    val pos3 = Array(2, 19, 22)

    val p1 = new ExtentArray(pos1)
    val p2 = new ExtentArray(pos2)
    val p3 = new ExtentArray(pos3)

    mock1.expects('positions)().returning(p1)
    mock2.expects('positions)().returning(p2)
    mock3.expects('positions)().returning(p3)

    val ow = OrderedWindow(1, mock1, mock2, mock3)

    val expectedHits = new ExtentArray(Array(20))
    val h1 = ow.positions
    expectResult(expectedHits) {h1}

//    val ow1 = OrderedWindow(1, mock1, mock2, mock3)
//    val h2 = ow1.positions
//    expectResult(expectedHits) {h2}
  }
}
