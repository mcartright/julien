package julien
package behavior

/** This trait indicates that an operator implementation has fast random-access
  * capabilities. This means next, seek, and random-access operations are all
  * O(1).
  *
  * If this trait is not specified, the operator is assumed to have fast
  * ``next'' lookup, moderate ``seek'' cost, and high random-access cost.
  */
trait RandomAccess
