package julien
package retrieval

import julien.behavior.Movable

/** Root of all Features */
trait Feature extends Operator {
  type WeightType
  /** Provides a read-only view of the weight. Subtraits provide
    * mechanisms for setting the weight.
    */
  def weight: Double
  def weight_=(newWeight: WeightType): Unit
  def views: Set[View]
  def eval(id: InternalId): Double
}

/** Instantiates the weight of a [[Feature]] as publicly
  * exposed variable. Simplest implementation.
  */
trait ScalarWeightedFeature extends Feature {
  override type WeightType = Double
  protected var scalarWeight: Double = 1.0
  override def weight: Double = scalarWeight
  override def weight_=(newWeight: Double): Unit =
    this.scalarWeight = newWeight

  override def toString: String = {
    val b = new StringBuilder()
    b append stringPrefix append ":" append scalarWeight
    b append "(" append children.mkString(",")
    b append ")"
    b.result
  }
}

/** Instantiates the weight of a [[Feature]] as a settable
  * function. For now we assume the function takes zero parameters but produces
  * a double on demand.
  */
trait FunctionWeightedFeature extends Feature {
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
