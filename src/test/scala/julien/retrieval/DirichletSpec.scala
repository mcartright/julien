package julien
package retrieval

import org.scalatest._
import org.scalamock.scalatest.proxy.MockFactory
import julien._

object DirichletSpec {
  def countStats = CountStatistics(
    new CollFreq(10306507L),
    new NumDocs(25199354),
    new CollLength(13162442311L),
    new DocFreq(103045),
    new MaximumCount(345))
}

class DirichletSpec extends FlatSpec with MockFactory {
  def fixture = new {
    // Set up the needed mock objects
    val mockCV = mock[CountView]
    val mockLV = mock[LengthsView]
    val mockStat = mock[StatisticsView]
    val fakeCountStats = DirichletSpec.countStats
  }

  "A Dirichlet Scorer" should "calculate the correct coll freq" in {
    val f = fixture
    import f._

    mockStat.expects('statistics)().returning(fakeCountStats).noMoreThanOnce
    val d = Dirichlet(mockCV, mockLV, mockStat)
    val expectedCF =
      fakeCountStats.collFreq.toDouble / fakeCountStats.collLength
    expect(expectedCF) { d.cf }
  }

  it should "complain if it receives a negative mu" in {
    val f = fixture
    import f._

    intercept[IllegalArgumentException] {
      val d = Dirichlet(mockCV, mockLV, mockStat, -100)
    }
  }

  it should "produce the correct upper bound" in {
    val f = fixture
    import f._

    mockStat.expects('statistics)().returning(fakeCountStats).noMoreThanTwice
    val mu = 1633
    val d = Dirichlet(mockCV, mockLV, mockStat, mu)
    val max = fakeCountStats.max.toDouble
    val expScore = scala.math.log((max + (mu * d.cf)) / (max + mu))
    expect (expScore) { d.upperBound.underlying }
  }

  it should "produce the correct lower bound" in {
    val f = fixture
    import f._

    mockStat.expects('statistics)().returning(fakeCountStats).noMoreThanTwice
    val mu = 1800
    val d = Dirichlet(mockCV, mockLV, mockStat, mu)
    val expScore =
      scala.math.log((mu * d.cf) / (Dirichlet.totallyMadeUpValue + mu))
    expect (expScore) { d.lowerBound.underlying }
  }

  it should "produce the correct score" in {
    val f = fixture
    import f._

    val c = 5
    val l = 150
    val mu = 1750
    mockStat.expects('statistics)().returning(fakeCountStats).noMoreThanTwice
    mockCV.expects('count)().returning(c)
    mockLV.expects('length)().returning(l)
    val d = Dirichlet(mockCV, mockLV, mockStat, mu)
    val expScore = scala.math.log((c + (mu*d.cf)) / (mu + l))
    expect (expScore) { d.eval.underlying }
  }
}
