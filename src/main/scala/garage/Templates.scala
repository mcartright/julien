package garage

object Templates {
  def forAll(rv: RandomVariable) : RandomVariableGenerator = {
    return RandomVariableGenerator(rv)
  }
}
