package julien
package eval

import scala.collection.SeqProxy
import scala.math._
import scala.util.Random

// Let's see if this works... if so, should probably mixin IterableProxyLike
case class QueryResult[T <: ScoredObject](result: Seq[T])
extends SeqProxy[T] {
  def self = result

  def sample(sampleRate: Double): QueryResult[T] = {
    assume(sampleRate > 0.0 && sampleRate <= 1.0,
      s" Can't sample with a rate of $sampleRate")

    if (sampleRate == 1.0) return QueryResult(result)
    else {
      val newSize = round(result.size * sampleRate).toInt
      QueryResult(Random.shuffle(result).take(newSize))
    }
  }

  def rankEqual(that: QueryResult[T]): Boolean =
    if (this.length != that.length)
      false
    else
      this.zip(that).forall(p => p._1.id == p._2.id)

  def scoreEqual(that: QueryResult[T]): Boolean =
    if (this.length != that.length)
      false
    else
      this.zip(that).forall(p => p._1.id == p._2.id && p._1.score == p._2.score)
}
