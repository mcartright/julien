package julien.learning.linear

import org.scalatest._

class CoordinateAscentSpec extends FlatSpec with ShouldMatchers {
  val qrels = ""

  "A Coordinate Ascent" should "maximize a 3d parabola just fine" in {
    val eval2d = new Evaluation2D(13.0, 26.0)
    val coordAscent = new CoordinateAscent(eval2d)

    coordAscent.train

    eval2d.x.eval should be (13.0 plusOrMinus 1.0)
    eval2d.y.eval should be (26.0 plusOrMinus 1.0)
  }
}
