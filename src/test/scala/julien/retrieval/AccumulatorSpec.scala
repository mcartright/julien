package julien
package retrieval

import org.scalatest._
import scala.util.Random
import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer

trait StandardAccumulatorBehavior { this: FlatSpec =>
  def aStandardAccumulator[T <: ScoredObject](
    genericAcc: Accumulator[T],
    universeSz: Int,
    requested: Int
  ) {
    val clue = s"|U| = $universeSz, |R| = $requested"

    it should "return the requested number of scoreables" in {
      if (genericAcc.isInstanceOf[ArrayAccumulator[_]])
        cancel("ArrayAcc needs work")
      val acc = genericAcc.asInstanceOf[Accumulator[ScoredDocument]]
      // Make some fake samples
      val limit = Random.nextInt(universeSz)+1
      val samples = ArrayBuffer[ScoredDocument]()
      for (i <- 0 until limit) {
        val sd = ScoredDocument(
          id = i,
          score = Random.nextDouble*100
        )
        samples += sd
        acc += sd
      }

      // Make the reference list
      val topRanked = samples.result.sorted.take(requested)
      val results = acc.result

      withClue(clue) {
        expectResult(topRanked.size)(results.size)
        for ((sd1, sd2) <- topRanked.zip(results)) {
          expectResult(sd1)(sd2)
        }
      }
    }
  }
}

class AccumulatorSpec
    extends FlatSpec
    with StandardAccumulatorBehavior {

  // Still allows for tests where universe < requested
  val requested = Random.nextInt(1000) + 10
  val universeSz = Random.nextInt(requested * 100) + 1

  val defAcc = DefaultAccumulator[ScoredDocument](requested)
  "A DefaultAccumulator" should
  behave like aStandardAccumulator(defAcc, universeSz, requested)

  val arrayAcc = ArrayAccumulator[ScoredDocument](universeSz, requested)

  "An ArrayAccumulator" should
  behave like aStandardAccumulator(arrayAcc, universeSz, requested)
}
