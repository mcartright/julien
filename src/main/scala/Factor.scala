case class Factor(variables: RandomVariable*)
case class FactorGenerator(rvg: RandomVariableGenerator, factors: List[Factor] = List.empty) {
  def this(f: List[Factor]) = this(null, f)

  override def toString() : String = if (rvg == null) {
    this.getClass.getName + "(" + factors.mkString(",") + ")"
  } else {
    this.getClass.getName + "(" + rvg.toString + ")"
  }
}

object Factor {
  def dirichlet(name: RandomVariable, src: RandomVariable) = termFunction(name, src)
  def jm(name: RandomVariable, src: RandomVariable) = termFunction(name, src)
  def bm25(name: RandomVariable, src: RandomVariable) = termFunction(name, src)
  def termFunction(name: RandomVariable, src: RandomVariable) : Factor = new Factor(name, src)
} 

