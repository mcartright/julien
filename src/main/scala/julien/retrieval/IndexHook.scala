package julien
package retrieval

/** Behavior needed by anything that connects to an index.
  * The Index itself is expected to be provided at construction
  * time for any implementing traits.
  */
trait IndexHook {
  /** The index attached to this hook. */
  val index: Index
}
