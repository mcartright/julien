package julien
package retrieval

import org.scalatest._
import org.scalamock.scalatest.proxy.MockFactory
import julien._

object BM25Spec {
  def countStats = CountStatistics(
    new CollFreq(10306507L),
    new NumDocs(25199354),
    new CollLength(13162442311L),
    new DocFreq(103045),
    new MaximumCount(345))
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

    mockStat.expects('statistics)().returning(fakeCountStats).noMoreThanOnce
    val d = BM25(mockCV, mockLV, mockStat)
    val expectedADL =
      fakeCountStats.collLength.toDouble / fakeCountStats.numDocs
    expect(expectedADL) { d.avgDocLength }
  }

  it should "complain if it receives a negative b parameter" in (pending)
  it should "complain if it receives a negative k parameter" in (pending)

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
    expect (expScore) { d.upperBound.underlying }
  }

  it should "produce the correct lower bound" in {
    val f = fixture
    import f._

    mockStat.expects('statistics)().returning(fakeCountStats).noMoreThanTwice
    val d = BM25(mockCV, mockLV, mockStat)
    expect (0) { d.lowerBound.underlying }
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
    expect (expScore) { d.eval.underlying }
  }
}
