package julien
package retrieval
package processor

import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer
import julien.getType
import julien.behavior.Distributive


/** Simple query flattener. Given this layout:
 * - P is the parent node
 * - N is the current node
 * - C1...CJ are the j children nodes
 *
 * Look for situations where
 * - P.class >:> N.class (N is a subclass of P)
 * - N is Distributive
 *
 * That way we can simply re-weight N's children and
 * reattach them to the parent node, elminating N entirely.
 *
 * This flattening behavior is done in Galago as well, but is
 * restricted to certain node classes.
 */
object Flattener extends QueryRewriter {
  def rewrite(root: Feature): Feature = {
    def rewriteSubtree(node: Feature) {
      if (node.children.isEmpty) return

      for (c <- node.children) rewriteSubtree(c)

      if (node.isInstanceOf[Distributive] &&
	  node.children.exists(_.isInstanceOf[Distributive])) {
	    val nodeType = getType(node)
	    var newChildren = ArrayBuffer[Feature]()
	    for (c <- node.children) {
	      if (nodeType =:= getType(c)) {
		newChildren ++= c.asInstanceOf[Distributive].distribute
	      } else {
		newChildren += c
	      }
	    }
	    val d = node.asInstanceOf[Distributive]
	    d.setChildren(newChildren)
	  }
    }
    rewriteSubtree(root)
    return root
  }
}
