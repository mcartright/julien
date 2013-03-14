/** Defines a trait that allows for retrieval of objects
  */
trait Retrievable[RU, KeyT] {
  def get(key : KeyT) : Option[RU]
}
