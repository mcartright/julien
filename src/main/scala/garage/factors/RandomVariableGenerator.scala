package garage
package factors

object RandomVariableGenerator {
  def apply(rv: RandomVariable) = new RandomVariableGenerator(rv)
  def apply(rvs: List[RandomVariable]) = new RandomVariableGenerator(rvs)
}

class RandomVariableGenerator(rv: RandomVariable, instances: List[RandomVariable] = List.empty) extends RandomVariable {
  def this(i: List[RandomVariable]) = this(null, i)

  /** Creates a new FactorGenerator that uses the attached function to convert instances of RandomVariables
    * to instances of Factors
    *
    */
  def map(f: RandomVariable => Factor) : FactorGenerator = FactorGenerator(this, f)

  /** Returns an estimate of the size of the number of variables to be generated.
    *
    */
  def size = rv match {
    case null => instances.size
    case _ => 1
  }

  override def toString() : String = if (rv == null) {
    this.getClass.getName + "(" + instances.mkString(",") + ")"
  } else {
    this.getClass.getName + "(" + rv.toString + ")"
  }
}
