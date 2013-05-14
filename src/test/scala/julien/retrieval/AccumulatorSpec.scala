package julien
package retrieval

import org.scalatest._
import scala.util.Random
import scala.reflect.runtime.universe._

trait StandardAccumulatorBehavior { this: FlatSpec =>
  def aStandardAccumulator[T <: ScoredObject[T]](
    acc: Accumulator[T],
    universeSz: Int,
    requested: Int
  ) {
    it should "return the requested number of scoreables" in (pending)
  }

  // Would REALLY like to make this work over the different subtypes of
  // ScoredObject
  val scoreable = typeTag[ScoredDocument]
}

class AccumulatorSpec
    extends FlatSpec
    with StandardAccumulatorBehavior {

  def defAcc[T <: ScoredObject[T]](ev: TypeTag[T]) = DefaultAccumulator[T]()
  def arrayAcc[T <: ScoredObject[T]](ev: TypeTag[T], idxSz: Int, sz: Int) =
    ArrayAccumulator[T](idxSz, sz)

  "A DefaultAccumulator" should
  behave like aStandardAccumulator(defAcc(scoreable), 1000, 10)

  "An ArrayAccumulator" should
  behave like aStandardAccumulator(arrayAcc(scoreable, 1000 ,10), 1000, 10)

}
