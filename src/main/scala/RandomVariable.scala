import org.lemurproject.galago.core.index.Index

abstract class RandomVariable
case class Term(t: String, src: Index) extends RandomVariable
case class RandomVariableInt(i: Int) extends RandomVariable
case class RandomVariableFloat(f: Float) extends RandomVariable
case class IndexDocument(src: Index) extends RandomVariable
case class Result(f: FactorGenerator) extends RandomVariable

object RandomVariable {
  def indexTerm(t: String, src: Index) = Term(t, src)
  def document(src: Index) = IndexDocument(src)
  def int(i: Int) = RandomVariableInt(i)
  def float(f: Float) = RandomVariableFloat(f)
}

object RandomVariableGenerator {
  def apply(rv: RandomVariable) = new RandomVariableGenerator(rv)
  def apply(rvs: List[RandomVariable]) = new RandomVariableGenerator(rvs)
}

class RandomVariableGenerator(rv: RandomVariable, instances: List[RandomVariable] = List.empty) extends RandomVariable { 
  def this(i: List[RandomVariable]) = this(null, i)
  def map[B](f: RandomVariable => B) : FactorGenerator = new FactorGenerator(this) 

  override def toString() : String = if (rv == null) {
    this.getClass.getName + "(" + instances.mkString(",") + ")"
  } else {
    this.getClass.getName + "(" + rv.toString + ")"
  }
}
