package julien
package retrieval
package processor

/** Generic trait for functions that rewrite queries.
 */
trait QueryRewriter {
  def rewrite(root: Feature): Feature
}
