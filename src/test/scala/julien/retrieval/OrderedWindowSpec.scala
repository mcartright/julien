package julien
package retrieval

import org.scalatest._
import org.scalamock.scalatest.proxy.MockFactory
import julien._

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
  }
}
