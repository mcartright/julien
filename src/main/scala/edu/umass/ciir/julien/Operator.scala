package edu.umass.ciir.julien

import scala.collection.{Traversable,TraversableLike}
import scala.collection.immutable.List
import scala.collection.mutable.{Builder,ListBuffer,Queue}
import scala.collection.generic.CanBuildFrom

trait Operator extends Traversable[Operator] {
  def children: Seq[Operator]
  def foreach[U](f: Operator => U) = {
    f(this)
    // ironically...
    for (c <- children) c foreach f
  }

  override def toString: String = {
    val b = new StringBuilder()
    b append stringPrefix append "("
    b append children.mkString(",")
    b append ")"
    b.result
  }

  override def newBuilder: Builder[Operator, List[Operator]] =
    Operator.newBuilder
}

object Operator {
  implicit def canBuildFrom: CanBuildFrom[Operator, Operator, List[Operator]] =
    new CanBuildFrom[Operator, Operator, List[Operator]] {
      def apply(): Builder[Operator, List[Operator]] = newBuilder
      def apply(from: Operator): Builder[Operator, List[Operator]] = newBuilder
    }
  def newBuilder: Builder[Operator, List[Operator]] = ListBuffer[Operator]()
}

// Views

// Views provide Values to the Features
trait ViewOp extends Operator
trait CountView extends ViewOp with CountSrc with StatisticsSrc
trait PositionsView extends CountView with PositionSrc
trait DataView[T] extends ViewOp with DataSrc[T]
trait ScoreView extends ViewOp with ScoreSrc
trait ChildlessOp extends Operator {
  lazy val children: Seq[Operator] = List.empty
  // Ever so slightly faster here
  override def foreach[U](f: Operator => U) = f(this)
}

// Root of all Features
trait FeatureOp extends Operator {
  def views: Set[ViewOp]
  def eval: Score
}

// A FeatureView is basically a store-supplied feature -
// basically anything precomputed.
trait FeatureView extends ViewOp with FeatureOp

// Overloaded operators that do both -- these need work
// case class Require[T](test: (_) => Boolean, op: Operator) extends FeatureOp
// case class Reject[T](test: (_) => Boolean, op: Operator) extends FeatureOp
// case class Priors extends FeatureOp
