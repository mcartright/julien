package julien
package retrieval

import org.scalatest._
import org.scalamock.scalatest.proxy.MockFactory
import julien._

object BM25Spec {
  def countStats = CountStatistics(
    collFreq = 10306507L,
    numDocs = 25199354,
    collLength = 13162442311L,
    docFreq = 103045,
    max = 345)
}

class BM25Spec extends FlatSpec with MockFactory {
  def fixture = new {
    // Set up the needed mock objects
    val mockCV = mock[CountView]
    val mockLV = mock[LengthsView]
    val mockStat = mock[StatisticsView]
    val fakeCountStats = BM25Spec.countStats
  }

  "A BM25 Scorer" should "calculate the correct avg doc length" in {
    val f = fixture
    import f._

    mockStat.expects('statistics)().
      returning(fakeCountStats).noMoreThanOnce
    val d = BM25(mockCV, mockLV, mockStat)
    val expectedADL =
      fakeCountStats.collLength.toDouble / fakeCountStats.numDocs
    expectResult(expectedADL) { d.avgDocLength }
  }

  it should "complain if it receives a b parameter outside interval [0,1]" in {
    val f = fixture
    import f._

    intercept[IllegalArgumentException] {
      val d = BM25(mockCV, mockLV, mockStat, -0.1, 34)
    }

    intercept[IllegalArgumentException] {
      val d = BM25(mockCV, mockLV, mockStat, 3.4, 110)
    }
  }

  it should "complain if it receives a negative k parameter" in {
    val f = fixture
    import f._

    intercept[IllegalArgumentException] {
      val d = BM25(mockCV, mockLV, mockStat, 0.7, -1.1)
    }
  }

  it should "calculate the correct inv. doc. freq. (IDF)" in {
    val f = fixture
    import f._

    mockStat.expects('statistics)().returning(fakeCountStats).noMoreThanOnce
    val d = BM25(mockCV, mockLV, mockStat)
    val expectedIDF = scala.math.log(
      fakeCountStats.numDocs.toDouble / (fakeCountStats.docFreq + 0.5)
    )
    expect(expectedIDF) { d.idf }
  }

  it should "produce the correct upper bound" in {
    val f = fixture
    import f._

    mockStat.expects('statistics)().returning(fakeCountStats).noMoreThanTwice
    val b = 0.395
    val k = 1.9
    val d = BM25(mockCV, mockLV, mockStat, b, k)
    val max = fakeCountStats.max.toDouble
    val numerator = max + (k + 1)
    val denominator = max + (k * (1 - b + (b * max / d.avgDocLength)));
    val expScore = d.idf * numerator / denominator
    expect (expScore) { d.upperBound }
  }

  it should "produce the correct lower bound" in {
    val f = fixture
    import f._

    mockStat.expects('statistics)().returning(fakeCountStats).noMoreThanTwice
    val d = BM25(mockCV, mockLV, mockStat)
    expect (0) { d.lowerBound }
  }

  it should "produce the correct score" in {
    val f = fixture
    import f._

    val c = 5
    val l = 150
    val b = 0.99
    val k = 2.3
    mockStat.expects('statistics)().returning(fakeCountStats).noMoreThanTwice
    mockCV.expects('count)().returning(c)
    mockLV.expects('length)().returning(l)
    val d = BM25(mockCV, mockLV, mockStat, b, k)
    val numerator = c + (k + 1)
    val denominator = c + (k * (1 - b + (b * l / d.avgDocLength)));
    val expScore = d.idf * numerator / denominator
    expect (expScore) { d.eval }
  }
}
