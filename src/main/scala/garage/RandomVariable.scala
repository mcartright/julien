package garage

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
