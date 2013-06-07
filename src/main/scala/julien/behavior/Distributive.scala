package julien
package behavior

import julien.retrieval.Feature

/** Indicates that if a Feature has children, the effect the Feature
  * has on its children can be applied once, and instead we can
  * operate directly on its children during evaluation. Basically, we
  * call this once, and then save on the call overhead for the rest of
  * the evaluation.
  */
trait Distributive {
  /** Distributes the effect of this operator. The returned tuple contains
    * 1) the modified sequence of children, and
    * 2) An operation that can fold the sequence together into
    *    the single value that would have been returned by the original
    *    eval call to this operator. Note that the signature of the operation
    *    indicates that the operation be commutative (i.e. the order in which we
    *    apply the operation to the children should not matter). If this
    *    assumption cannot be met, do not mix in this trait.
    */
  def distribute: (Seq[Feature], (Feature, InternalId, Double) => Double)
}
