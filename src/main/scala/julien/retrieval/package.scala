package julien

import language.implicitConversions
import reflect.runtime.universe._
import collection.JavaConversions._
import collection.mutable.Set

/** High-level container of the "retrieval" behaviors in Julien.
  * This package contains all the definitions for operators (views
  * and features), as well as the processor classes necessary to
  * execute a query or batch of queries.
  *
  * This package makes substantial use of the functionality provided
  * by the access API.
  *
  * Running a single query:
  *
  * {{{
  * // Import the retrieval package
  * scala> import julien.retrieval._
  * import julien.retrieval._
  *
  * // Open an index. We set it as implicit to provide defaults
  * // to all the operators that need it.
  * scala> implicit val index = Index.disk("./myJulienIndex")
  * index: julien.access.Index = disk:./myJulienIndex
  *
  * // Create a query-likelihood query. `Dirichlet.apply` is the
  * // scoring function applied over each query term.
  * scala> val ql = bow(List("new", "york", "city"), Dirichlet.apply)
  * ql: julien.retrieval.Feature = CombineNorm(Dirichlet(new: ...
  *
  * Make a processor for the query
  * scala> val processor = SimpleProcessor()
  * processor: julien.retrieval.SimpleProcessor = julien.retrieval.SimpleProcessor@611c048e
  *
  * // Add the query to the processor
  * scala> processor add ql
  *
  * // Run and get results
  * scala> val results = processor.run()
  * results: julien.eval.QueryResult[julien.retrieval.ScoredDocument] = List(...
  * }}}
  */
package object retrieval {
  implicit def featSeq2Array(s: Seq[Feature]): Array[Feature] = s.toArray
  implicit val sdOrdering = ScoredDocumentDefaultOrdering

  // Do some type aliasing on objects
  val Combine = Sum
  val CombineNorm = NormalizedSum

  // Bring in local references to some of the access structures
  type Index = julien.access.Index
  val Index = julien.access.Index

  import julien.{galago => G}
  // Stopwords here - somewhere else?
  object Stopwords {
    lazy val inquery: Set[String] =
      G.tupleflow.Utility.readStreamToStringSet(
        classOf[G.core.index.Index].getResourceAsStream("/stopwords/inquery")
      )
  }

  def getType[T](x: T)(implicit tag: TypeTag[T]) = tag.tpe

  // Basic query operations - subject to moving
  def bow(
    terms: Seq[String],
    scorer: (Term, IndexLengths) => Feature)
  (implicit index: Index): ScalarWeightedFeature = {
    CombineNorm(terms.map(t =>
      scorer(Term(t)(index), IndexLengths()(index))))
  }

  def sdm(terms: String, weight: Double)
    (implicit index: Index): ScalarWeightedFeature = {
    val node = sdm(terms.split(" "), Dirichlet.wrap _)(index)
    node.weight = weight
    node
  }

  def sdm(
    rawterms: Seq[String],
    scorer: (CountStatsView, LengthsView) => Feature,
    unigramWeight: Double = 0.8,
    odWeight: Double = 0.15,
    uwWeight: Double = 0.05,
    odWindowSize: Int = 1,
    uwWindowSize: Int = 8)
    (implicit index: Index): ScalarWeightedFeature = {
    val terms = rawterms.map(Term.positions(_)(index))
    if (terms.length == 1) {
      Combine(Seq(scorer(terms(0), IndexLengths())))
    } else {
      Combine(
        // List of unigram, od, and uw features
        List[Feature](
          // unigram feature
          CombineNorm(children = terms.map(a => scorer(a,IndexLengths())),
            weight = unigramWeight),
          // ordered window feature
          CombineNorm(children = terms.sliding(2,1).map { p =>
            scorer(OrderedWindow(odWindowSize, p: _*), IndexLengths())
          }.toSeq, weight = odWeight),
          // unordered window feature
          CombineNorm(terms.sliding(2,1).map { p =>
            scorer(UnorderedWindow(uwWindowSize, p: _*), IndexLengths())
          }.toSeq, weight = uwWeight)
        )
      )
    }
  }
}
