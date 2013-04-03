package edu.umass.ciir.julien

import scala.collection.{Traversable,TraversableLike}
import scala.collection.immutable.List
import scala.collection.mutable.{Builder,ListBuffer,Queue}
import scala.collection.generic.CanBuildFrom
import edu.umass.ciir.julien.Aliases._

// Two kinds of operations on a query graph:
// 1) views : Masquerade one operator for 1 or more, or for filtering
// 2) features: Use views to perform calculations to provide some belief
//              of the current system state.
trait Operator
    extends Traversable[Operator]
    with TraversableLike[Operator, List[Operator]]
{
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

// // Views provide Values to the Features
trait ViewOp extends Operator
trait CountOp extends ViewOp with CountSrc with StatisticsSrc
trait PositionsOp extends CountOp with PositionSrc
trait DataOp[T] extends ViewOp with DataSrc[T]
trait ScoreOp extends ViewOp with ScoreSrc

// Features
sealed trait FeatureOp extends Operator { def views: Set[ViewOp] }
trait TraversableEvaluator[T] extends FeatureOp { def eval(obj: T): Score }
trait IntrinsicEvaluator extends FeatureOp { def eval: Score }
trait CLEvaluator extends FeatureOp { def eval(c: Count, l: Length): Score }
trait LengthsEvaluator extends FeatureOp { def eval(l: Length): Score }

// Overloaded operators that do both -- these need work
// case class Require[T](test: (_) => Boolean, op: Operator) extends FeatureOp
// case class Reject[T](test: (_) => Boolean, op: Operator) extends FeatureOp
// case class Priors extends FeatureOp



