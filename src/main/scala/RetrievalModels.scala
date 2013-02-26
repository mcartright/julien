import scala.collection.immutable.List
import org.lemurproject.galago.core.index.disk.DiskIndex

object RetrievalModels {
  type ReduceFunction = FactorGenerator => RandomVariableGenerator
  type TermFactorFunction = (RandomVariable, RandomVariable) => Factor

  // Almost all of this needs further definition. 
  def product(factors: FactorGenerator) : RandomVariableGenerator = return RandomVariableGenerator(Result(factors))
  def sum(factors: FactorGenerator) : RandomVariableGenerator = return RandomVariableGenerator(Result(factors))

  def bm25(indexPath: String, query: Query) = bagOfWords(indexPath, query)(Factor.bm25, sum)
  def dirichlet(indexPath: String, query: Query) = bagOfWords(indexPath, query)(Factor.dirichlet)
  def jm(indexPath: String, query: Query) = bagOfWords(indexPath, query)(Factor.jm)

  def bagOfWords(indexPath: String, query: Query)(f: TermFactorFunction, r: ReduceFunction = product) : RandomVariableGenerator = {
    // Load our index
    val index = new DiskIndex(indexPath)
    val unigramVariables = RandomVariableGenerator("""\w+""".r.findAllIn(query.text).map { T => RandomVariable.indexTerm(T, index) }.toList)
    val doc = Templates.forAll(RandomVariable.document(index))
    val factors : FactorGenerator = unigramVariables.map { U => f(U, doc) }
    // Select the top 100 results after using a standard ranking
    return Operation.topK(100)(Operation.sort(Operation.reduce(r)(factors)))
  }

  def relevanceModel(indexPath: String, query: Query) : RandomVariableGenerator = {
    // Load our index
    val index = new DiskIndex(indexPath)
    val lm = RetrievalModels.dirichlet(indexPath, query)
    // This selects a set of weighted terms (random variables) using the LanguageModel variable
    val termSelectionFactor = Factor(lm, RandomVariable.int(10), RandomVariable.int(100))
    val selectedTerms = Operation.topK(10)(Operation.filter(Operation.reduce(product)(termSelectionFactor)))

    // Now make a language model based on the selected terms
    val doc = Templates.forAll(RandomVariable.document(index))
    val factors : FactorGenerator = selectedTerms.map { U => Factor.dirichlet(U, doc) }
    return Operation.topK(100)(Operation.sort(Operation.reduce(product)(factors)))
  }

  implicit def factor2factorgen(f: Factor) : FactorGenerator = new FactorGenerator(List(f))
}
