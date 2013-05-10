package julien
package learning

import scala.util.Random

package object neuralnet {
  def unitGaussian: Double =
    Random.nextDouble * (if (Random.nextBoolean) 1 else -1)
}
