package julien
package retrieval

import scala.collection.{Traversable,TraversableLike}
import scala.collection.immutable.List
import scala.collection.mutable.{Builder,ListBuffer,Queue}
import scala.collection.generic.CanBuildFrom
import scala.reflect.runtime.universe._

trait Operator extends Traversable[Operator] {
  def children: Seq[Operator]
  def foreach[U](f: Operator => U) = {
    f(this)
    // ironically...
    for (c <- children) c foreach f
  }

  // It works, but for generic types (i.e. passing in A[_]) causes
  // debug spam to show up. Use at your own risk for now :)
  def grab[T](implicit tag: TypeTag[T]): Traversable[T] = {
    val m = runtimeMirror(this.getClass.getClassLoader)
    this.
      filter(item => m.reflect(item).symbol.selfType <:< tag.tpe).
      map(_.asInstanceOf[T]).
      toList
  }

  def movers: Traversable[Movable] = grab[Movable]

  def iHooks: Traversable[IteratedHook[_ <: GIterator]] = this.
    filter(_.isInstanceOf[IteratedHook[_ <: GIterator]]).
    map(_.asInstanceOf[IteratedHook[_ <: GIterator]]).
    toList

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

/** Views provide values to the Features, but this line is
  *  blurry.
  */
trait ViewOp extends Operator
trait BooleanView extends ViewOp with BoolSrc
trait CountView extends ViewOp with CountSrc
trait StatisticsView extends ViewOp with StatisticsSrc
trait PositionsView extends CountView with PositionSrc
trait PositionStatsView extends PositionsView with StatisticsView
trait LengthsView extends ViewOp with LengthsSrc
trait DataView[T] extends ViewOp with DataSrc[T]
trait ChildlessOp extends Operator {
  lazy val children: Seq[Operator] = List.empty
  // Ever so slightly faster here
  override def foreach[U](f: Operator => U) = f(this)
}

/** Root of all Features */
trait FeatureOp extends Operator {
  type WeightType
  /** Provides a read-only view of the weight. Subtraits provide
    * mechanisms for setting the weight.
    */
  def weight: Double
  def weight_=(newWeight: WeightType): Unit
  def views: Set[ViewOp]
  def eval: Double
  def upperBound: Double = Double.PositiveInfinity
  def lowerBound: Double = Double.NegativeInfinity
}

/** Instantiates the weight of a [[FeatureOp]] as publicly
  * exposed variable. Simplest implementation.
  */
trait ScalarWeightedFeature extends FeatureOp {
  override type WeightType = Double
  protected var scalarWeight: Double = 1.0
  override def weight: Double = scalarWeight
  override def weight_=(newWeight: Double): Unit =
    this.scalarWeight = newWeight
}

/** Instantiates the weight of a [[FeatureOp]] as a settable
  * function. For now we assume the function takes zero parameters but produces
  * a double on demand.
  */
trait FunctionWeightedFeature extends FeatureOp {
  override type WeightType = () => Double
  private val defWeightFn = () => 1.0
  protected var weightFn: Option[() => Double] = None
  def weightFunction: () => Double = weightFn.getOrElse(defWeightFn)
  override def weight: Double = weightFn.getOrElse(defWeightFn)()
  def weight_=(scalar: Double): Unit = weight = () => scalar
  override def weight_=(newWeight: () => Double): Unit = {
    weightFn = Some(newWeight)
  }
}

/** A FeatureView is a store-supplied feature -basically anything precomputed,
  * but that definition is quite loose.
  */
trait FeatureView extends ViewOp with FeatureOp with ChildlessOp

// Overloaded operators that do both -- these need work
// case class Require[T](test: (_) => Boolean, op: Operator) extends FeatureOp
// case class Reject[T](test: (_) => Boolean, op: Operator) extends FeatureOp

