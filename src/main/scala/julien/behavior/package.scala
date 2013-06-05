package julien

/** This package contains the various traits recognized by the processor
  * when performing optimizations.
  *
  * As long as an operator implements the methods described by the
  * [[julien.retrieval.Feature Feature]] trait, it can be used in retrieval
  * models. However, further annotating an operator with the traits contained
  * in this package ensures the system of further assumptions that cannot be
  * inferred automatically. These assumptions will allow for more aggressive
  * run-time optimization of retrieval models involving that operator.
  */
package object behavior {
}
