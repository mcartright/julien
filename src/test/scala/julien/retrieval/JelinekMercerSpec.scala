package julien
package retrieval

import org.scalatest._
import org.scalamock.scalatest.proxy.MockFactory
import julien._

object JelinekMercerSpec {
  def countStats = CountStatistics(
    collFreq = 10306507L,
    numDocs = 25199354,
    collLength = 13162442311L,
    docFreq = 103045,
    max = 345)
}

class JelinekMercerSpec extends FlatSpec with MockFactory {
  def fixture = new {
    // Set up the needed mock objects
    val mockCV = mock[CountView]
    val mockLV = mock[LengthsView]
    val mockStat = mock[StatisticsView]
    val fakeCountStats = JelinekMercerSpec.countStats
  }

  "A JelinekMercer Scorer" should "calculate the correct coll freq" in {
    val f = fixture
    import f._

    mockStat.expects('statistics)().returning(fakeCountStats).noMoreThanOnce
    val lambda = 0.8
    val d = JelinekMercer(mockCV, mockLV, mockStat, lambda)
    val expectedCF =
      fakeCountStats.collFreq.toDouble / fakeCountStats.collLength
    expectResult(expectedCF) { d.cf }
  }

  it should "complain if the provided lambda is outside [0,1]" in {
    val f = fixture
    import f._

    intercept[IllegalArgumentException] {
      val jm = JelinekMercer(mockCV, mockLV, mockStat, 1.2)
    }

    intercept[IllegalArgumentException] {
      val jm = JelinekMercer(mockCV, mockLV, mockStat, -0.03)
    }
  }

  it should "produce the correct upper bound" in {
    val f = fixture
    import f._

    mockStat.expects('statistics)().returning(fakeCountStats).noMoreThanTwice
    val lambda = 0.5
    val d = JelinekMercer(mockCV, mockLV, mockStat, lambda)
    val max = fakeCountStats.max.toDouble
    val expScore = scala.math.log(lambda + ((1.0-lambda) * d.cf))
    expectResult(expScore) { d.upperBound }
  }

  it should "produce the correct lower bound" in {
    val f = fixture
    import f._

    mockStat.expects('statistics)().returning(fakeCountStats).noMoreThanTwice
    val lambda = 0.3
    val d = JelinekMercer(mockCV, mockLV, mockStat, lambda)
    val expScore = scala.math.log((1.0-lambda) * d.cf)
    expectResult(expScore) { d.lowerBound }
  }

  it should "produce the correct score" in {
    val f = fixture
    import f._

    val c = 23
    val l = 312
    val lambda = 0.45
    val id = 15
    mockStat.expects('statistics)().returning(fakeCountStats).noMoreThanTwice
    mockCV.expects('count)(id).returning(c)
    mockLV.expects('length)(id).returning(l)
    val d = JelinekMercer(mockCV, mockLV, mockStat, lambda)
    val expScore =
      scala.math.log((lambda*(c.toDouble/l)) + ((1.0-lambda)*(d.cf)))
    expectResult(expScore) { d.eval(id) }
  }
}
