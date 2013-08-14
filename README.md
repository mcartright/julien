julien
======

Julien is a retrieval stack built for performing experiments in Information Retrieval research.
The current version of Julien is 0.1, mostly because it's been under development and I haven't had
time to *really* set it up for a release. Right now the documentation is spotty, but I will be
shoring it up in the coming weeks. The scaladocs can be found at http://ayr.cs.umass.edu/julien-docs/julien .

# Model

Julien is primarily driven by [Operator](http://ayr.cs.umass.edu/julien-docs/julien/#julien.retrieval.Operator)s
and [Processor](http://ayr.cs.umass.edu/julien-docs/julien/#julien.retrieval.processor.QueryProcessor)s. 

Operators are broken down into [View](http://ayr.cs.umass.edu/julien-docs/julien/#julien.retrieval.View)s and
[Feature](http://ayr.cs.umass.edu/julien-docs/julien/#julien.retrieval.Feature)s - views provide channels of information
from the index, and features are functions that operate on information provided to eventually produce scores for
documents. 

Processors encapsulate the logic needed to run a tree of operators over the whole index and return a ranked list 
of documents. There are other components in making a system, but those two are the most central.

# Example: Running a Single Query

Julien was designed to require little outside of specifying the query. In a sense, the philosophy is
"if you make the query, it'll run". 

```scala
import julien.retrieval._

// Implicit so the index is supplied where needed without you having to fill it in
implicit val index = Index.disk("path_to_my_index")

// Our query...
val querytext = "pros and cons of the hyperloop"

// Going to transform it by hand - this can always be wrapped in a function
val root = Combine(
  querytext.split(" ").map { t => 
    Dirichlet(Term(t), IndexLengths())
  }
)

// Let the object create a processor to run it, and return results
val results = QueryProcessor(root)
```

# Adding a new Operator

If you're into creating new retrieval models, then you're going to want your own operators.
A good place to start is under the [Operator](http://ayr.cs.umass.edu/julien-docs/julien/#julien.retrieval.Operator)
documentation, which describes the root of all the things used to construct queries.
