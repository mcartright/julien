package julien.learning
package jforests.learning

package object trees {
  type Ensemble = scala.collection.mutable.ArrayBuffer[Tree]
  object Ensemble {
    def apply(): Ensemble = new Ensemble
    def apply(t: Tree) = new Ensemble(t)
    def apply(e: Ensemble) = new Ensemble(e)
  }
}
