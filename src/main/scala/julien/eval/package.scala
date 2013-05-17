package julien

import eval._
import scala.math._
import scala.collection.immutable.TreeMap
import scala.Predef.Map

package object eval {
  type QuerySetEvaluation = TreeMap[String, Double]
  type QueryJudgmentSet = Map[String, QueryJudgments]

  // These are carried over from the 'aggregate' package of Galago's eval
//  def gMean(values: Array[Double]): Double =
//    pow(values.product, 1.0 / values.size)
//
//  def aMean(values: Array[Double]): Double = values.sum / values.size
//  def mean(values: Array[Double]) = aMean(values)
}
