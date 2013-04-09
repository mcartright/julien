package julien

/** Encapsulation of a single posting in the index.
  * An Index object can produce a [julien.seq.PostingSeq]]
  * over these given a ViewOp.
  */
trait Posting[T <: Posting[T]] {
  def docid: Docid
  def copy: T
}

//TODO: Implement this? Unclear - it's so generic, may not be worth it.
// trait DataPosting extends Posting with DataSrc
