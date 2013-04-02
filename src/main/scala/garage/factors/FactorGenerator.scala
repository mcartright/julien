package garage.factors

import scala.collection.immutable.List

object FactorGenerator {
  def apply(rvg: RandomVariableGenerator, mapFn: RandomVariable => Factor) = new FactorGenerator(rvg, mapFn)
  def apply(f: List[Factor]) = new FactorGenerator(f)
}

class FactorGenerator(rvg: RandomVariableGenerator, mapFn: RandomVariable => Factor, factors: List[Factor] = List.empty) {
  def this(f: List[Factor]) = this(null, null, f)

  override def toString() : String = if (rvg == null) {
    this.getClass.getName + "(" + factors.mkString(",") + ")"
  } else {
    this.getClass.getName + "(" + mapFn.toString + "," + rvg.toString + ")"
  }
}
