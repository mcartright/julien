package julien

import org.scalatest._
import julien.galago.core.util.ExtentArray

trait PositionsBehaviors { this: FlatSpec =>
  def emptyPositions(emptyP: => Positions) {
    it should "have no elements" in {
      assert(emptyP.length === 0)
    }

    it should "throw an exception if we access anything in it" in {
      intercept[ArrayIndexOutOfBoundsException] {
        emptyP(4)
      }
    }
  }

  def nonEmptyPositions(pos: => Positions, expLength: Int) {
    it should "match the expected length" in {
      expect(expLength) { pos.length }
     }

    it should "allow indexed access" in {
      for (i <- 0 until pos.length) {
        val v = pos(i)
      }
    }

    it should "throw an exception if we go out of range" in {
      intercept[ArrayIndexOutOfBoundsException] {
        pos(pos.length+3)
      }
    }
  }
}

class PositionsSpec
    extends FlatSpec
    with GivenWhenThen
    with PositionsBehaviors {
  "Positions" should "have an 'empty' placeholder" in {
      assert((Positions.empty != null) === true)
    }
  it should behave like emptyPositions(Positions.empty)

  val emptyP = Positions(Array[Int]())
  "An empty Positions array" should behave like emptyPositions(emptyP)

  val arr = Array(1,3,5,7,11,45)
  val fullP = Positions(arr)
  "A non-empty Positions array" should
    behave like nonEmptyPositions(fullP, arr.size)

  val b = new scala.collection.mutable.ArrayBuffer[Int]
  b += 3 += 5
  val sz = b.size
  val bufp = Positions(b)
  "Positions built from an ArrayBuffer" should
    behave like nonEmptyPositions(bufp, sz)

  it should "be immutable" in {
    When("we add something to the buffer")
    b += 7

    Then("it shouldn't affect the positions")
    assert(bufp.contains(7) === false)
  }
  "The same positions" should behave like nonEmptyPositions(bufp, sz)

  val ea = new ExtentArray(5)
  ea.add(1)
  ea.add(2)
  ea.add(5)
  ea.add(7)
  ea.add(12)
  val eap = Positions(ea)
  "Positions built from an ExtentArray" should
  behave like nonEmptyPositions(eap, ea.size)
}
