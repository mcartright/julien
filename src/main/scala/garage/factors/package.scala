package garage

/** This attempt was from a very Graphical Model point of view.
  * Hence we have the [[Factor]] and [[RandomVariable]] classes
  * that form the basis of the model.
  *
  * Unfortunately it wasn't clear how to connect the underlying
  * index structure to an arbitrary model, and we kept trying to
  * fully express constructs like query iteration and execution
  * over the whole collection as a single graph construct. Didn't
  * really become clear how to do that while staying in the Graphical
  * Model world.
  *
  * Ulimately, I think the current incarnation of the system provides
  * a set of models that encompass what is expressible in a GM
  * framework. My hope is to provide some examples of doing this.
  */
package object factors {
  type Index = org.lemurproject.galago.core.index.Index
  type DiskIndex = org.lemurproject.galago.core.index.disk.DiskIndex
}
