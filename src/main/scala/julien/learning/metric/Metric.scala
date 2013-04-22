package julien
package learning
package metric

object Metric extends Enumeration {
  type Metric = Value
  val MAP, NDCG, DCG, P, RR, BEST, ERR = Value
}
