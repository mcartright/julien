package julien
package retrieval

import scala.collection.{Traversable,TraversableLike}
import scala.collection.immutable.List
import scala.collection.mutable.{Builder,ListBuffer,Queue}
import scala.collection.generic.CanBuildFrom
import scala.reflect.runtime.universe._
import julien.behavior._

trait Operator extends Traversable[Operator] {
  def children: Seq[Operator]
  def foreach[U](f: Operator => U) = {
    f(this)
    for (c <- children) c foreach f
  }

  // It works, but for generic types (i.e. passing in A[_]) causes
  // debug spam to show up. Use at your own risk for now :)
  def grab[T](implicit tag: TypeTag[T]): Seq[T] = {
    val m = runtimeMirror(this.getClass.getClassLoader)
    this.
      filter(item => m.reflect(item).symbol.selfType <:< tag.tpe).
      map(_.asInstanceOf[T]).
      toList
  }

  def movers: Seq[Movable] = grab[Movable]
  def iHooks: Seq[IteratedHook[_ <: GIterator]] =
    grab[IteratedHook[_ <: GIterator]]
  def hooks: Seq[IndexHook] = grab[IndexHook]

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
  import language.implicitConversions
  // Slight simplification
  implicit def op2feature(op: Operator): Feature = op.asInstanceOf[Feature]
  implicit def op2view(op: Operator): View = op.asInstanceOf[View]

  implicit def canBuildFrom: CanBuildFrom[Operator, Operator, List[Operator]] =
    new CanBuildFrom[Operator, Operator, List[Operator]] {
      def apply(): Builder[Operator, List[Operator]] = newBuilder
      def apply(from: Operator): Builder[Operator, List[Operator]] = newBuilder
    }
  def newBuilder: Builder[Operator, List[Operator]] = ListBuffer[Operator]()
}

// Views
trait ChildlessOp extends Operator {
  lazy val children: Seq[Operator] = List.empty
  // Ever so slightly faster here
  override def foreach[U](f: Operator => U) = f(this)
}



