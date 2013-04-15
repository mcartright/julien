package julien
package retrieval

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

  def grab[T]: Traversable[T] = this.
    filter(_.isInstanceOf[T]).
    map(_.asInstanceOf[T]).
    toList

  def iHooks: Traversable[IteratedHook[_ <: GIterator]] =
    grab[IteratedHook[_ <: GIterator]]

  def hooks: Traversable[IndexHook] = this.
    filter(_.isInstanceOf[IndexHook]).
    map(_.asInstanceOf[IndexHook]).
    toList

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
  // Slight simplification
  implicit def op2feature(op: Operator): FeatureOp = op.asInstanceOf[FeatureOp]
  implicit def op2view(op: Operator): ViewOp = op.asInstanceOf[ViewOp]

  implicit def canBuildFrom: CanBuildFrom[Operator, Operator, List[Operator]] =
    new CanBuildFrom[Operator, Operator, List[Operator]] {
      def apply(): Builder[Operator, List[Operator]] = newBuilder
      def apply(from: Operator): Builder[Operator, List[Operator]] = newBuilder
    }
  def newBuilder: Builder[Operator, List[Operator]] = ListBuffer[Operator]()
}

// Views

// Views provide Values to the Features
trait ViewOp extends Operator {
  def size: Int
  def isDense: Boolean
  def isSparse: Boolean = !isDense
}
trait BooleanView extends ViewOp with BoolSrc
trait CountView extends ViewOp with CountSrc with StatisticsSrc
trait PositionsView extends CountView with PositionSrc
trait DataView[T] extends ViewOp with DataSrc[T]
trait ChildlessOp extends Operator {
  lazy val children: Seq[Operator] = List.empty
  // Ever so slightly faster here
  override def foreach[U](f: Operator => U) = f(this)
}

// Root of all Features
trait FeatureOp extends Operator {
  def views: Set[ViewOp]
  def eval: Score
  def upperBound: Score = new Score(Double.PositiveInfinity)
  def lowerBound: Score = new Score(Double.NegativeInfinity)
}

// A FeatureView is a store-supplied feature -
// basically anything precomputed.
trait FeatureView extends ViewOp with FeatureOp with ChildlessOp

// This is a marker trait that indicates the need for preparation
// prior to execution. A typical example is an OrderedWindow view
// needs to calculate some statistics before scoring takes place to
// ensure accurate scoring
trait NeedsPreparing {
  def updateStatistics
}

// Overloaded operators that do both -- these need work
// case class Require[T](test: (_) => Boolean, op: Operator) extends FeatureOp
// case class Reject[T](test: (_) => Boolean, op: Operator) extends FeatureOp

