package julien

import scala.reflect.runtime.universe._

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
  * ql: julien.retrieval.FeatureOp = CombineNorm(Dirichlet(new: ...
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
  type Combiner = (InternalId, Seq[FeatureOp]) => Double
  type QueryPreparer = (String) => Seq[FeatureOp]

  // Bring in local references to some of the access structures
  type Index = julien.access.Index
  val Index = julien.access.Index

  import julien.{galago => G}
  // Stopwords here - somewhere else?
  object Stopwords {
    def inquery =
      G.tupleflow.Utility.readStreamToStringSet(
        classOf[G.core.index.Index].getResourceAsStream("/stopwords/inquery")
      )
  }

  def getType[T](x: T)(implicit tag: TypeTag[T]) = tag.tpe

  // Basic query operations - subject to moving
  def bow(
    terms: Seq[String],
    scorer: (Term, IndexLengths) => FeatureOp)
  (implicit index: Index): FeatureOp = {
    CombineNorm(terms.map(t =>
      scorer(Term(t)(index), IndexLengths()(index))))
  }

  def sdm(
    rawterms: Seq[String],
    scorer: (Term, IndexLengths) => FeatureOp,
    unigramWeight: Double = 0.8,
    odWeight: Double = 0.15,
    uwWeight: Double = 0.05,
    odWindowSize: Int = 1,
    uwWindowSize: Int = 8)
    (implicit index: Index): FeatureOp = {
    val terms = rawterms.map(Term(_)(index))
    return Combine(
      // List of unigram, od, and uw features
      List[FeatureOp](
        // unigram feature
        CombineNorm(children = terms.map(a => Dirichlet(a,IndexLengths())),
          weight = unigramWeight),
        // ordered window feature
        CombineNorm(children = terms.sliding(2,1).map { p =>
          Dirichlet(OrderedWindow(odWindowSize, p: _*), IndexLengths())
        }.toSeq, weight = odWeight),
        // unordered window feature
        CombineNorm(terms.sliding(2,1).map { p =>
          Dirichlet(UnorderedWindow(uwWindowSize, p: _*), IndexLengths())
        }.toSeq, weight = uwWeight)
      )
    )
  }
}
