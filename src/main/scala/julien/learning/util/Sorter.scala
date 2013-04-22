package julien
package learning
package util

object Sorter {
  def sort(values: List[_ <: Numeric], asc: Boolean): List[Int] = if (asc)
    values.zipWithIndex.sortBy(_._1).map(_._2)
  else
    values.zipWithIndex.sortBy(_._1).map(_._2).reverse
}
