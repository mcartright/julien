case class ReduceOp(r: FactorGenerator => RandomVariableGenerator, factors: FactorGenerator) extends RandomVariableGenerator(Result(factors))
case class SelectOp(rv: RandomVariableGenerator) extends RandomVariableGenerator(rv)
case class FilterOp(rv: RandomVariableGenerator) extends RandomVariableGenerator(rv)
case class SortOp(rv: RandomVariableGenerator) extends RandomVariableGenerator(rv)
case class TopKOp(rv: RandomVariableGenerator) extends RandomVariableGenerator(rv)

object Operation {
  def reduce(r: RetrievalModels.ReduceFunction)(factors: FactorGenerator) : RandomVariableGenerator = {
    return ReduceOp(r, factors)
  }

  def select(vars: RandomVariableGenerator) : RandomVariableGenerator = {
    return SelectOp(vars)
  }

  def filter(vars : RandomVariableGenerator) : RandomVariableGenerator = {
    return FilterOp(vars)
  }
  
  def sort(vars : RandomVariableGenerator) : RandomVariableGenerator = {
    return SortOp(vars)
  }

  def topK(limit: Int)(incoming: RandomVariableGenerator) : RandomVariableGenerator = {
    return TopKOp(incoming)
  }
}
