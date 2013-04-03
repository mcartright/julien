package edu.umass.ciir.julien

import scala.collection.{IndexedSeqOptimized,IndexedSeqLike}
import scala.collection.generic._
import scala.collection.immutable.Vector
import scala.collection.mutable.{ArrayBuffer,Builder}
import org.lemurproject.galago.core.util.ExtentArray

class Positions(underlying: Array[Int])
    extends IndexedSeqOptimized[Int, Positions] {
  def apply(idx: Int): Int = underlying(idx)
  def length: Int = underlying.length
  def seq: IndexedSeq[Int] = Vector(underlying: _*)
  def newBuilder : Builder[Int, Positions] = Positions.newBuilder
}

object Positions {
  val empty = new Positions(Array.empty)
  implicit def canBuildFrom: CanBuildFrom[Positions, Int, Positions] =
    new CanBuildFrom[Positions, Int, Positions] {
      def apply(): Builder[Int, Positions] = newBuilder
      def apply(from: Positions): Builder[Int, Positions] = newBuilder
    }
  def apply() = Positions.empty
  def apply(a: Array[Int]) = new Positions(a)
  def apply(a: ArrayBuffer[Int]) = new Positions(a.toArray)
  def apply(e: ExtentArray) : Positions = {
    val b = newBuilder
    for (i <- 0 until e.size) b += e.begin(i)
    b.result
  }
  def newBuilder: Builder[Int, Positions] = new ArrayBuffer[Int] mapResult apply
}
