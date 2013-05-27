package julien
package access

/** Encapsulation of a single posting in the index.
  * An Index object can produce a [julien.seq.PostingSeq]]
  * over these given a ViewOp.
  */
trait Posting[T <: Posting[T]] {
  def docid: InternalId
  def copy: T
}
