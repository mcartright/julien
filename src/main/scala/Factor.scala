object Factor {
  def dirichlet(name: RandomVariable, src: RandomVariable) = termFunction(name, src)
  def jm(name: RandomVariable, src: RandomVariable) = termFunction(name, src)
  def bm25(name: RandomVariable, src: RandomVariable) = termFunction(name, src)
  def termFunction(name: RandomVariable, src: RandomVariable) : Factor = new Factor(name, src)
} 

case class Factor(variables: RandomVariable*)
