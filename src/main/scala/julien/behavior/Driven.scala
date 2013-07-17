package julien
package behavior

import language.implicitConversions

object Driven {
  implicit def drvToMv(d: Driven): Movable = d.driver
}

trait Driven {
  val driver: Movable
}
